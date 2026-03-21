const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const Ring = @import("ring.zig").Ring;
const op_slot = @import("op_slot.zig");
const OpSlot = op_slot.OpSlot;
const OpSlotTable = op_slot.OpSlotTable;
const conn_mod = @import("connection.zig");
const Connection = conn_mod.Connection;
const ConnectionPool = conn_mod.ConnectionPool;
const ConnectionState = conn_mod.ConnectionState;
const http = @import("http.zig");
const hparse = @import("hparse");
const http_response = @import("http_response.zig");
const config_mod = @import("config.zig");
const ArenaConfig = config_mod.ArenaConfig;
const chunk_pool = @import("chunk_pool.zig");
const ChunkRecycler = chunk_pool.ChunkRecycler;
const arena_mod = @import("arena.zig");
const RequestArena = arena_mod.RequestArena;
const BufferRecycler = @import("buffer_recycler.zig").BufferRecycler;

const lazy_request = @import("lazy_request.zig");

// ---------------------------------------------------------------------------
// Zig 0.16 compat: posix.close/write/socket were removed from std.posix.
// Use raw linux syscalls instead (this is a Linux-only io_uring framework).
// ---------------------------------------------------------------------------

fn linuxClose(fd: posix.fd_t) void {
    _ = linux.close(fd);
}

fn linuxWrite(fd: posix.fd_t, buf: []const u8) !usize {
    const rc = linux.write(fd, buf.ptr, buf.len);
    return if (posix.errno(rc) != .SUCCESS)
        error.WriteFailed
    else
        rc;
}

fn linuxSocket(domain: u32, socket_type: u32, protocol: u32) !posix.fd_t {
    const rc = linux.socket(domain, socket_type, protocol);
    return if (posix.errno(rc) != .SUCCESS)
        error.SocketFailed
    else
        @intCast(rc);
}

fn linuxPipe() ![2]posix.fd_t {
    var fds: [2]i32 = undefined;
    return if (posix.errno(linux.pipe(&fds)) != .SUCCESS)
        error.PipeFailed
    else
        fds;
}

/// Build a sockaddr_in from IPv4 bytes + port (replaces std.net.Address.initIp4).
fn makeSockaddrIn4(addr_bytes: [4]u8, port: u16) posix.sockaddr {
    const addr: posix.sockaddr.in = .{
        .family = posix.AF.INET,
        .port = std.mem.nativeToBig(u16, port),
        .addr = @bitCast(addr_bytes),
        .zero = [_]u8{0} ** 8,
    };
    return @bitCast(addr);
}

const py = @cImport({
    @cInclude("py_helpers.h");
});

const PyObject = py.PyObject;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const MAX_FDS: usize = 4096;
const STOP_SENTINEL: u64 = std.math.maxInt(u64);
const ACCEPT_SENTINEL: u64 = std.math.maxInt(u64) - 1;
const IGNORE_SENTINEL: u64 = std.math.maxInt(u64) - 2;
const CQE_BATCH_SIZE: usize = 256;
const MAX_POLL_FDS: usize = 64;
const IOSQE_IO_LINK: u8 = 1 << 2;
const RECV_SIZE: usize = 8192;
const MAX_HEADERS: usize = 100;

// ---------------------------------------------------------------------------
// Hub struct
// ---------------------------------------------------------------------------

pub const Hub = struct {
    ring: Ring,
    pool: ConnectionPool,
    op_slots: OpSlotTable,
    fd_to_conn: [MAX_FDS]?u16, // fd → pool_index (for looking up connections by fd)
    // Accept state (listen fd is not in the connection pool)
    accept_greenlet: ?*PyObject,
    accept_result: i32,
    accept_pending: bool,
    ready: std.ArrayListUnmanaged(?*PyObject), // ready queue
    hub_greenlet: ?*PyObject, // the hub greenlet itself
    stop_pipe: [2]posix.fd_t, // pipe for thread-safe stop signaling
    stop_buf: [1]u8, // read buffer for stop pipe
    active_waits: usize, // count of pending I/O operations
    running: bool,
    allocator: std.mem.Allocator,
    config: ArenaConfig,
    recycler: ChunkRecycler,
    buf_recycler: BufferRecycler,

    pub fn init(allocator: std.mem.Allocator, config: ArenaConfig) !Hub {
        const ring = try Ring.init(256);
        errdefer {
            var r = ring;
            r.deinit();
        }

        var recycler = ChunkRecycler.init(allocator);
        errdefer recycler.deinit();

        var buf_recycler = BufferRecycler.init(allocator);
        errdefer buf_recycler.deinit();

        var pool = try ConnectionPool.init(allocator, config, &recycler, &buf_recycler);
        errdefer pool.deinit();

        var op_slots = try OpSlotTable.init(allocator);
        errdefer op_slots.deinit();

        const pipe_fds = try linuxPipe();

        return Hub{
            .ring = ring,
            .pool = pool,
            .op_slots = op_slots,
            .fd_to_conn = [_]?u16{null} ** MAX_FDS,
            .accept_greenlet = null,
            .accept_result = 0,
            .accept_pending = false,
            .ready = .empty,
            .hub_greenlet = null,
            .stop_pipe = .{ pipe_fds[0], pipe_fds[1] },
            .stop_buf = .{0},
            .active_waits = 0,
            .running = false,
            .allocator = allocator,
            .config = config,
            .recycler = recycler,
            .buf_recycler = buf_recycler,
        };
    }

    pub fn deinit(self: *Hub) void {
        // Decref accept greenlet
        if (self.accept_greenlet) |g| {
            py.py_helper_decref(g);
            self.accept_greenlet = null;
        }

        // Decref greenlets in active connections
        // (arena cleanup is handled by pool.deinit() -> arena.reset())
        for (self.pool.connections) |*conn| {
            if (conn.greenlet) |g_opaque| {
                const g: *PyObject = @ptrCast(@alignCast(g_opaque));
                py.py_helper_decref(g);
                conn.greenlet = null;
            }
        }

        // Decref greenlets in op slots
        for (&self.op_slots.slots) |*slot| {
            if (slot.greenlet) |g_opaque| {
                const g: *PyObject = @ptrCast(@alignCast(g_opaque));
                py.py_helper_decref(g);
                slot.greenlet = null;
            }
        }
        self.op_slots.deinit();

        // Decref ready queue entries
        for (self.ready.items) |maybe_g| {
            if (maybe_g) |g| {
                py.py_helper_decref(g);
            }
        }
        self.ready.deinit(self.allocator);

        // Close stop pipe
        linuxClose(self.stop_pipe[0]);
        linuxClose(self.stop_pipe[1]);

        // Decref hub greenlet
        if (self.hub_greenlet) |g| {
            py.py_helper_decref(g);
            self.hub_greenlet = null;
        }

        // Pool must deinit before recyclers: pool.deinit() calls
        // arena.reset() which returns chunks/buffers to the recyclers.
        self.pool.deinit();
        self.recycler.deinit();
        self.buf_recycler.deinit();
        self.ring.deinit();
    }

    // -----------------------------------------------------------------------
    // Hub loop
    // -----------------------------------------------------------------------

    pub fn hubLoop(self: *Hub) void {
        self.running = true;

        // Submit a read SQE on stop_pipe[0] to detect stop signals
        _ = self.ring.prepRead(self.stop_pipe[0], &self.stop_buf, STOP_SENTINEL) catch {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "hub: failed to submit stop pipe read",
            );
            return;
        };

        while (self.running) {
            // a. DRAIN READY QUEUE
            var idx: usize = 0;
            while (idx < self.ready.items.len) {
                const maybe_g = self.ready.items[idx];
                idx += 1;
                if (maybe_g) |g| {
                    const result = py.py_helper_greenlet_switch(g, null, null);
                    py.py_helper_decref(g);
                    if (result == null) {
                        if (py.py_helper_err_occurred() != null) {
                            while (idx < self.ready.items.len) {
                                if (self.ready.items[idx]) |remaining| {
                                    py.py_helper_decref(remaining);
                                }
                                idx += 1;
                            }
                            self.ready.clearRetainingCapacity();
                            return;
                        }
                        continue;
                    }
                    py.py_helper_decref(result);
                }
            }
            self.ready.clearRetainingCapacity();

            // b. EXIT CHECK
            if (self.active_waits == 0 and self.ready.items.len == 0) break;

            // c. SUBMIT + WAIT: release GIL, block until at least 1 CQE
            {
                const saved = py.py_helper_save_thread();
                const sw_result = self.ring.submitAndWait(1);
                py.py_helper_restore_thread(saved);
                _ = sw_result catch {
                    py.py_helper_err_set_string(
                        py.py_helper_exc_runtime_error(),
                        "hub: submitAndWait failed",
                    );
                    return;
                };
            }

            // d. BATCH DRAIN CQEs
            var cqe_buf: [CQE_BATCH_SIZE]linux.io_uring_cqe = undefined;
            const cqes = self.ring.copyCqes(&cqe_buf, 0) catch {
                py.py_helper_err_set_string(
                    py.py_helper_exc_runtime_error(),
                    "hub: copyCqes failed",
                );
                return;
            };

            // e. Process each CQE
            for (cqes) |cqe| {
                if (cqe.user_data == IGNORE_SENTINEL) continue;

                if (cqe.user_data == STOP_SENTINEL) {
                    self.running = false;
                    continue;
                }

                if (cqe.user_data == ACCEPT_SENTINEL) {
                    // Accept completion
                    self.accept_result = cqe.res;
                    self.accept_pending = false;
                    self.active_waits -= 1;
                    if (self.accept_greenlet) |g| {
                        self.accept_greenlet = null;
                        self.ready.append(self.allocator, g) catch {
                            py.py_helper_decref(g);
                        };
                    }
                    continue;
                }

                // Check for op slot CQE (bit 63 set)
                if (op_slot.isOpSlot(cqe.user_data)) {
                    const slot = self.op_slots.lookup(cqe.user_data) orelse continue;
                    slot.result = cqe.res;

                    if (slot.poll_group) |group| {
                        // Multi-poll slot: only the first completion resumes
                        if (!group.resumed) {
                            group.resumed = true;
                            if (group.greenlet) |g_opaque| {
                                const g: *PyObject = @ptrCast(@alignCast(g_opaque));
                                self.active_waits -= group.active_waits_n;
                                self.ready.append(self.allocator, g) catch {
                                    py.py_helper_decref(g);
                                };
                            }
                        }
                        // Don't decref greenlet here — PollGroup owns the single ref
                    } else if (slot.greenlet) |g_opaque| {
                        // Regular single-poll slot (existing path)
                        const g: *PyObject = @ptrCast(@alignCast(g_opaque));
                        slot.greenlet = null;
                        self.active_waits -= 1;
                        self.ready.append(self.allocator, g) catch {
                            py.py_helper_decref(g);
                        };
                    }
                    continue;
                }

                // Connection pool lookup with generation validation
                const conn = self.pool.lookup(cqe.user_data) orelse {
                    // Stale CQE — generation mismatch or invalid index, discard
                    continue;
                };

                conn.result = cqe.res;
                if (conn.pending_ops > 0) conn.pending_ops -= 1;

                // Handle cancel CQE (during cancel-before-close)
                if (conn.state == .cancelling) {
                    if (conn.pending_ops == 0) {
                        // All cancelled, now submit close
                        self.submitClose(conn);
                    }
                    continue;
                }

                // Handle close CQE
                if (conn.state == .closing) {
                    self.releaseConnection(conn);
                    continue;
                }

                // Normal I/O completion — wake the greenlet
                conn.state = .idle;
                if (conn.greenlet) |g_opaque| {
                    const g: *PyObject = @ptrCast(@alignCast(g_opaque));
                    conn.greenlet = null;
                    self.active_waits -= 1;
                    self.ready.append(self.allocator, g) catch {
                        py.py_helper_decref(g);
                    };
                }
            }
        }
    }

    fn submitClose(self: *Hub, conn: *Connection) void {
        conn.state = .closing;
        const user_data = conn_mod.encodeUserData(conn.pool_index, conn.generation);
        _ = self.ring.prepClose(conn.fd, user_data) catch {
            // If we can't submit close, just release directly
            self.releaseConnection(conn);
            return;
        };
        conn.pending_ops += 1;
    }

    fn releaseConnection(self: *Hub, conn: *Connection) void {
        const fd_usize: usize = @intCast(conn.fd);
        if (fd_usize < MAX_FDS) {
            self.fd_to_conn[fd_usize] = null;
        }
        // Decref greenlet if present
        if (conn.greenlet) |g_opaque| {
            const g: *PyObject = @ptrCast(@alignCast(g_opaque));
            py.py_helper_decref(g);
            conn.greenlet = null;
        }
        // pool.release() calls conn.reset() which calls arena.reset()
        self.pool.release(conn);
    }

    // -----------------------------------------------------------------------
    // sendRejectAndClose: send an HTTP error response and close the fd
    // -----------------------------------------------------------------------

    /// Send a rejection HTTP response (e.g. 503 Service Unavailable) and
    /// close the file descriptor. Used for connection pool exhaustion and
    /// other hard-limit violations. Uses blocking posix write (not io_uring)
    /// since this is a fast-path rejection — the fd is not in the pool.
    pub fn sendRejectAndClose(_: *Hub, fd: posix.fd_t, status_code: u16) void {
        const status = statusFromInt(status_code) orelse return;
        var buf: [256]u8 = undefined;
        const n = http_response.writeResponse(&buf, status, &.{}, "") catch return;
        _ = linuxWrite(fd, buf[0..n]) catch {};
        linuxClose(fd);
    }

    // -----------------------------------------------------------------------
    // Switch failure cleanup helpers
    // -----------------------------------------------------------------------

    /// Connection-based switch failure cleanup. Idempotent with the CQE handler:
    /// if conn.greenlet is null, the CQE handler already did everything.
    /// Returns true if cleanup was performed (CQE not yet processed).
    ///
    /// Does NOT decref the greenlet — this branch also runs during dealloc
    /// switches (GreenletExit), where the greenlet is mid-dealloc with
    /// refcnt=1. Decrefing would trigger a nested green_dealloc and crash.
    /// The ref is owned by deinit, which decrefs all conn.greenlet entries.
    fn cleanupConnSwitch(self: *Hub, conn: *Connection) bool {
        const g_opaque = conn.greenlet orelse return false;
        _ = @as(*PyObject, @ptrCast(@alignCast(g_opaque)));
        conn.greenlet = null;
        conn.state = .idle;
        conn.pending_ops -= 1;
        self.active_waits -= 1;
        return true;
    }

    /// Op-slot-based switch failure cleanup. Always releases the slot.
    /// Idempotent with the CQE handler: if slot.greenlet is null, the CQE
    /// handler already decremented active_waits and consumed the greenlet ref.
    ///
    /// Does NOT decref the greenlet — this branch also runs during dealloc
    /// switches (GreenletExit), where the greenlet is mid-dealloc with
    /// refcnt=1. Decrefing would trigger a nested green_dealloc and crash.
    /// The ref is owned by deinit, which decrefs all slot.greenlet entries.
    fn cleanupSlotSwitch(self: *Hub, slot: *OpSlot) void {
        if (slot.greenlet) |_| {
            slot.greenlet = null;
            self.active_waits -= 1;
        }
        self.op_slots.release(slot);
    }

    // -----------------------------------------------------------------------
    // green_accept: submit accept SQE, yield, return new fd
    // -----------------------------------------------------------------------

    pub fn greenAccept(self: *Hub, listen_fd: posix.fd_t) ?*PyObject {
        // Get current greenlet
        const current = py.py_helper_greenlet_getcurrent() orelse return null;

        // Submit accept SQE with ACCEPT_SENTINEL user_data
        _ = self.ring.prepAccept(listen_fd, ACCEPT_SENTINEL) catch {
            py.py_helper_decref(current);
            py.py_helper_err_set_string(
                py.py_helper_exc_oserror(),
                "green_accept: failed to submit accept SQE",
            );
            return null;
        };

        self.accept_greenlet = current;
        self.accept_pending = true;
        self.active_waits += 1;

        // Switch to hub (suspends here)
        const hub_g = self.hub_greenlet orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_accept: hub greenlet not set",
            );
            self.accept_greenlet = null;
            self.accept_pending = false;
            self.active_waits -= 1;
            py.py_helper_decref(current);
            return null;
        };
        const switch_result = py.py_helper_greenlet_switch(hub_g, null, null);
        if (switch_result == null) {
            // Do NOT decref or touch any state here.
            //
            // This branch runs in TWO scenarios:
            //   1. Normal switch failure (runtime error during greenlet_switch)
            //   2. Dealloc switch (GreenletExit thrown during green_dealloc)
            //
            // In scenario 2, the greenlet is being deallocated with refcnt=1.
            // Decrefing here triggers a nested green_dealloc, corrupting GC
            // tracking state and causing a segfault.
            //
            // In both scenarios, the greenlet ref and active_waits are owned
            // by the CQE handler (which will arrive and clean up) or by
            // Hub.deinit (which decrefs accept_greenlet if non-null).
            return null;
        }
        py.py_helper_decref(switch_result);

        // On resume: read result
        const res = self.accept_result;
        if (res < 0) {
            py.py_helper_err_set_from_errno(-res);
            return null;
        }

        // Register the new fd in the connection pool
        const new_fd: posix.fd_t = @intCast(res);
        const new_fd_usize: usize = @intCast(new_fd);
        if (new_fd_usize >= MAX_FDS) {
            self.sendRejectAndClose(new_fd, 503);
            return py.PyLong_FromLong(@as(c_long, res));
        }

        const conn = self.pool.acquire() orelse {
            // Pool exhausted — reject with 503 and close the fd
            self.sendRejectAndClose(new_fd, 503);
            return py.PyLong_FromLong(@as(c_long, res));
        };
        conn.fd = new_fd;
        conn.state = .idle;
        self.fd_to_conn[new_fd_usize] = conn.pool_index;

        return py.PyLong_FromLong(@as(c_long, res));
    }

    // -----------------------------------------------------------------------
    // getOrCreateConn: look up connection by fd
    // -----------------------------------------------------------------------

    fn getConn(self: *Hub, fd: posix.fd_t) ?*Connection {
        const fd_usize: usize = @intCast(fd);
        if (fd_usize >= MAX_FDS) return null;
        const pool_idx = self.fd_to_conn[fd_usize] orelse return null;
        return &self.pool.connections[pool_idx];
    }

    // -----------------------------------------------------------------------
    // green_recv: submit recv SQE, yield, return bytes
    // -----------------------------------------------------------------------

    pub fn greenRecv(self: *Hub, fd: posix.fd_t, max_bytes: usize) ?*PyObject {
        const conn = self.getConn(fd) orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_oserror(),
                "green_recv: no connection for fd",
            );
            return null;
        };

        // Get writable space from the arena (no heap alloc)
        const buf = conn.arena.currentFreeSlice(max_bytes) orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_recv: out of memory",
            );
            return null;
        };

        // Get current greenlet
        const current = py.py_helper_greenlet_getcurrent() orelse {
            return null;
        };

        // Submit recv SQE with encoded user_data
        const user_data = conn_mod.encodeUserData(conn.pool_index, conn.generation);
        _ = self.ring.prepRecv(fd, buf, user_data) catch {
            py.py_helper_decref(current);
            py.py_helper_err_set_string(
                py.py_helper_exc_oserror(),
                "green_recv: failed to submit recv SQE",
            );
            return null;
        };

        conn.greenlet = current;
        conn.state = .reading;
        conn.pending_ops += 1;
        self.active_waits += 1;

        // Switch to hub
        const hub_g = self.hub_greenlet orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_recv: hub greenlet not set",
            );
            conn.greenlet = null;
            conn.state = .idle;
            conn.pending_ops -= 1;
            self.active_waits -= 1;
            py.py_helper_decref(current);
            return null;
        };
        const switch_result = py.py_helper_greenlet_switch(hub_g, null, null);
        if (switch_result == null) {
            _ = self.cleanupConnSwitch(conn);
            return null;
        }
        py.py_helper_decref(switch_result);

        // On resume: read result
        const res = conn.result;

        if (res < 0) {
            py.py_helper_err_set_from_errno(-res);
            return null;
        }

        const nbytes: usize = @intCast(res);

        // Commit the bytes to the arena (advance bump pointer)
        conn.arena.commitBytes(nbytes);

        // Copy into Python bytes object
        // No free needed -- arena.reset() handles everything on connection release
        return py.py_helper_bytes_from_string_and_size(
            @ptrCast(buf.ptr),
            @intCast(nbytes),
        );
    }

    // -----------------------------------------------------------------------
    // green_send: submit send SQE, yield, handle partial sends
    // -----------------------------------------------------------------------

    pub fn greenSend(self: *Hub, fd: posix.fd_t, py_bytes: *PyObject) ?*PyObject {
        const conn = self.getConn(fd) orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_oserror(),
                "green_send: no connection for fd",
            );
            return null;
        };

        // Get C buffer from Python bytes object
        const c_buf: [*]const u8 = @ptrCast(py.py_helper_bytes_as_string(py_bytes) orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_send: failed to get bytes buffer",
            );
            return null;
        });
        const buf_len: usize = @intCast(py.py_helper_bytes_get_size(py_bytes));

        // Incref the bytes object to prevent GC while SQE is in-flight
        py.py_helper_incref(py_bytes);

        // Get current greenlet
        const current = py.py_helper_greenlet_getcurrent() orelse {
            py.py_helper_decref(py_bytes);
            return null;
        };

        // Partial send loop
        var total_sent: usize = 0;
        while (total_sent < buf_len) {
            const remaining = c_buf[total_sent..buf_len];

            const user_data = conn_mod.encodeUserData(conn.pool_index, conn.generation);
            _ = self.ring.prepSend(fd, remaining, user_data) catch {
                if (total_sent == 0) py.py_helper_decref(current);
                py.py_helper_decref(py_bytes);
                py.py_helper_err_set_string(
                    py.py_helper_exc_oserror(),
                    "green_send: failed to submit send SQE",
                );
                return null;
            };

            conn.greenlet = current;
            conn.state = .writing;
            conn.pending_ops += 1;
            self.active_waits += 1;

            // Switch to hub
            const hub_g = self.hub_greenlet orelse {
                py.py_helper_err_set_string(
                    py.py_helper_exc_runtime_error(),
                    "green_send: hub greenlet not set",
                );
                conn.greenlet = null;
                conn.state = .idle;
                conn.pending_ops -= 1;
                self.active_waits -= 1;
                py.py_helper_decref(current);
                py.py_helper_decref(py_bytes);
                return null;
            };
            const switch_result = py.py_helper_greenlet_switch(hub_g, null, null);
            if (switch_result == null) {
                // Do NOT decref the greenlet — this branch also runs during
                // dealloc switches (GreenletExit). See cleanupConnSwitch.
                if (conn.greenlet) |_| {
                    conn.greenlet = null;
                    conn.state = .idle;
                    conn.pending_ops -= 1;
                    self.active_waits -= 1;
                }
                py.py_helper_decref(py_bytes);
                return null;
            }
            py.py_helper_decref(switch_result);

            const res = conn.result;
            if (res < 0) {
                py.py_helper_decref(py_bytes);
                py.py_helper_err_set_from_errno(-res);
                return null;
            }

            total_sent += @intCast(res);
        }

        py.py_helper_decref(py_bytes);
        return py.PyLong_FromLong(@as(c_long, @intCast(total_sent)));
    }

    // -----------------------------------------------------------------------
    // green_send_response: format headers in arena, writev headers + body
    // -----------------------------------------------------------------------

    /// Format response headers into the arena, then send headers + body
    /// via a single writev SQE. The body is never copied -- it is sent
    /// directly from the Python bytes object's buffer.
    pub fn greenSendResponse(
        self: *Hub,
        fd: posix.fd_t,
        status: http_response.StatusCode,
        zig_headers: []const http_response.Header,
        body_obj: *PyObject,
        body_slice: []const u8,
    ) ?*PyObject {
        const conn = self.getConn(fd) orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_oserror(),
                "http_send_response: no connection for fd",
            );
            return null;
        };

        // Estimate headers, allocate from arena, format
        const est = estimateHeaderSize(status, zig_headers, body_slice.len);
        const hdr_buf = conn.arena.alloc(est) orelse {
            _ = py.py_helper_err_no_memory();
            return null;
        };

        const hdr_len = http_response.writeResponseHead(
            hdr_buf,
            status,
            zig_headers,
            body_slice.len,
        ) catch {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "http_send_response: header format overflow (bug in estimateHeaderSize)",
            );
            return null;
        };

        // Build iovec array in the Connection struct (heap-resident).
        // IMPORTANT: iovecs MUST NOT be on the greenlet stack. When a greenlet
        // yields, the greenlet library copies the stack to a heap buffer and the
        // original stack address is reused by other greenlets. io_uring SQEs
        // store a pointer to the iovec array, so it must remain at a stable
        // address until the kernel processes the SQE.
        conn.send_iovecs[0] = .{ .base = hdr_buf.ptr, .len = hdr_len };
        var iov_count: usize = 1;
        if (body_slice.len > 0) {
            conn.send_iovecs[1] = .{ .base = body_slice.ptr, .len = body_slice.len };
            iov_count = 2;
        }

        // Incref body_obj to keep it alive during async send
        py.py_helper_incref(body_obj);

        const current = py.py_helper_greenlet_getcurrent() orelse {
            py.py_helper_decref(body_obj);
            return null;
        };

        // Partial writev loop (same pattern as greenSend)
        var iov_offset: usize = 0;
        var first_switch_done = false;
        while (iov_offset < iov_count) {
            // Skip fully-sent iovecs
            while (iov_offset < iov_count and conn.send_iovecs[iov_offset].len == 0) {
                iov_offset += 1;
            }
            if (iov_offset >= iov_count) break;

            const remaining_iovecs = conn.send_iovecs[iov_offset..iov_count];
            const user_data = conn_mod.encodeUserData(conn.pool_index, conn.generation);
            _ = self.ring.prepWritev(fd, remaining_iovecs, user_data) catch {
                py.py_helper_decref(body_obj);
                if (!first_switch_done) py.py_helper_decref(current);
                py.py_helper_err_set_string(
                    py.py_helper_exc_oserror(),
                    "http_send_response: failed to submit writev SQE",
                );
                return null;
            };

            conn.greenlet = current;
            conn.state = .writing;
            conn.pending_ops += 1;
            self.active_waits += 1;

            const hub_g = self.hub_greenlet orelse {
                py.py_helper_err_set_string(
                    py.py_helper_exc_runtime_error(),
                    "http_send_response: hub greenlet not set",
                );
                conn.greenlet = null;
                conn.state = .idle;
                conn.pending_ops -= 1;
                self.active_waits -= 1;
                py.py_helper_decref(current);
                py.py_helper_decref(body_obj);
                return null;
            };
            const switch_result = py.py_helper_greenlet_switch(hub_g, null, null);
            if (switch_result == null) {
                // Do NOT decref the greenlet — this branch also runs during
                // dealloc switches (GreenletExit). See cleanupConnSwitch.
                if (conn.greenlet) |_| {
                    conn.greenlet = null;
                    conn.state = .idle;
                    conn.pending_ops -= 1;
                    self.active_waits -= 1;
                }
                py.py_helper_decref(body_obj);
                return null;
            }
            first_switch_done = true;
            py.py_helper_decref(switch_result);

            const res = conn.result;
            if (res < 0) {
                py.py_helper_decref(body_obj);
                py.py_helper_err_set_from_errno(-res);
                return null;
            }

            // Advance iovecs past the bytes written
            var written: usize = @intCast(res);
            while (written > 0 and iov_offset < iov_count) {
                if (written >= conn.send_iovecs[iov_offset].len) {
                    written -= conn.send_iovecs[iov_offset].len;
                    conn.send_iovecs[iov_offset].len = 0;
                    iov_offset += 1;
                } else {
                    conn.send_iovecs[iov_offset].base += written;
                    conn.send_iovecs[iov_offset].len -= written;
                    written = 0;
                }
            }
        }

        py.py_helper_decref(body_obj);
        const total: usize = hdr_len + body_slice.len;
        return py.PyLong_FromLong(@as(c_long, @intCast(total)));
    }

    // -----------------------------------------------------------------------
    // green_close: cancel pending ops, close fd, release connection
    // -----------------------------------------------------------------------

    pub fn greenClose(self: *Hub, fd: posix.fd_t) ?*PyObject {
        const conn = self.getConn(fd) orelse {
            // No connection tracked — just close the fd directly via io_uring
            _ = self.ring.prepClose(fd, IGNORE_SENTINEL) catch {};
            const none = py.py_helper_none();
            py.py_helper_incref(none);
            return none;
        };

        if (conn.pending_ops > 0) {
            // Cancel pending operations first
            conn.state = .cancelling;
            const user_data = conn_mod.encodeUserData(conn.pool_index, conn.generation);
            // Submit async cancel for this connection's user_data
            _ = self.ring.prepCancel(user_data) catch {
                // If cancel fails, try to close directly
                self.submitClose(conn);
                const none = py.py_helper_none();
                py.py_helper_incref(none);
                return none;
            };
        } else {
            // No pending ops, go straight to close
            self.submitClose(conn);
        }

        const none = py.py_helper_none();
        py.py_helper_incref(none);
        return none;
    }

    // -----------------------------------------------------------------------
    // doRecv: internal recv helper for pyHttpReadRequest
    // -----------------------------------------------------------------------

    /// Submit a recv SQE for `buf`, suspend the greenlet, return bytes read.
    /// Returns null on error (Python exception already set).
    /// Returns 0 for EOF.
    fn doRecv(
        self: *Hub,
        fd: posix.fd_t,
        conn: *Connection,
        buf: []u8,
    ) ?usize {
        const current = py.py_helper_greenlet_getcurrent() orelse return null;

        const user_data = conn_mod.encodeUserData(conn.pool_index, conn.generation);
        _ = self.ring.prepRecv(fd, buf, user_data) catch {
            py.py_helper_decref(current);
            py.py_helper_err_set_string(
                py.py_helper_exc_oserror(),
                "doRecv: failed to submit recv SQE",
            );
            return null;
        };

        conn.greenlet = current;
        conn.state = .reading;
        conn.pending_ops += 1;
        self.active_waits += 1;

        const hub_g = self.hub_greenlet orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "doRecv: hub greenlet not set",
            );
            conn.greenlet = null;
            conn.state = .idle;
            conn.pending_ops -= 1;
            self.active_waits -= 1;
            py.py_helper_decref(current);
            return null;
        };
        const switch_result = py.py_helper_greenlet_switch(hub_g, null, null);
        if (switch_result == null) {
            _ = self.cleanupConnSwitch(conn);
            return null;
        }
        py.py_helper_decref(switch_result);

        const res = conn.result;
        if (res < 0) {
            py.py_helper_err_set_from_errno(-res);
            return null;
        }

        return @intCast(res);
    }

    // -----------------------------------------------------------------------
    // green_sleep: submit IORING_OP_TIMEOUT, yield, resume after timeout
    // -----------------------------------------------------------------------

    pub fn greenSleep(self: *Hub, seconds: f64) ?*PyObject {
        // Acquire an op slot
        const slot = self.op_slots.acquire() orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_sleep: op slot table exhausted",
            );
            return null;
        };

        // Get current greenlet
        const current = py.py_helper_greenlet_getcurrent() orelse {
            self.op_slots.release(slot);
            return null;
        };

        // Convert seconds to kernel_timespec — stored in the slot so it
        // survives greenlet stack swaps (io_uring reads the pointer at submit time).
        const whole_secs: i64 = @intFromFloat(seconds);
        const frac_nanos: i64 = @intFromFloat((seconds - @as(f64, @floatFromInt(whole_secs))) * 1_000_000_000.0);
        slot.storage = .{ .timespec = .{ .sec = whole_secs, .nsec = frac_nanos } };

        // Submit timeout SQE
        const user_data = op_slot.encodeUserData(slot.slot_index, slot.generation);
        _ = self.ring.prepTimeout(&slot.storage.timespec, user_data) catch {
            py.py_helper_decref(current);
            self.op_slots.release(slot);
            py.py_helper_err_set_string(
                py.py_helper_exc_oserror(),
                "green_sleep: failed to submit timeout SQE",
            );
            return null;
        };

        slot.greenlet = current;
        self.active_waits += 1;

        // Switch to hub (suspend)
        const hub_g = self.hub_greenlet orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_sleep: hub greenlet not set",
            );
            slot.greenlet = null;
            self.active_waits -= 1;
            py.py_helper_decref(current);
            self.op_slots.release(slot);
            return null;
        };
        const switch_result = py.py_helper_greenlet_switch(hub_g, null, null);
        if (switch_result == null) {
            self.cleanupSlotSwitch(slot);
            return null;
        }
        py.py_helper_decref(switch_result);

        // On resume: check result
        const res = slot.result;
        self.op_slots.release(slot);

        // -ETIME is the expected success case for timeout expiry
        const etime_neg = -@as(i32, @intFromEnum(linux.E.TIME));
        if (res != etime_neg and res < 0) {
            py.py_helper_err_set_from_errno(-res);
            return null;
        }

        const none = py.py_helper_none();
        py.py_helper_incref(none);
        return none;
    }

    // -----------------------------------------------------------------------
    // green_connect: create socket, submit CONNECT, yield, return fd
    // -----------------------------------------------------------------------

    pub fn greenConnect(self: *Hub, host: [*:0]const u8, port: u16) ?*PyObject {
        // Create TCP socket
        const fd = linuxSocket(posix.AF.INET, posix.SOCK.STREAM, 0) catch {
            py.py_helper_err_set_string(
                py.py_helper_exc_oserror(),
                "green_connect: failed to create socket",
            );
            return null;
        };

        // Parse host address
        const addr_bytes = parseIpv4(host) orelse {
            linuxClose(fd);
            py.py_helper_err_set_string(
                py.py_helper_exc_oserror(),
                "green_connect: invalid IPv4 address",
            );
            return null;
        };

        // Register fd in connection pool first
        const fd_usize: usize = @intCast(fd);
        if (fd_usize >= MAX_FDS) {
            linuxClose(fd);
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_connect: fd too large",
            );
            return null;
        }

        const conn = self.pool.acquire() orelse {
            linuxClose(fd);
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_connect: connection pool exhausted",
            );
            return null;
        };
        conn.fd = fd;
        conn.state = .idle;
        self.fd_to_conn[fd_usize] = conn.pool_index;

        // Acquire op slot for the connect operation
        const slot = self.op_slots.acquire() orelse {
            self.fd_to_conn[fd_usize] = null;
            self.pool.release(conn);
            linuxClose(fd);
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_connect: op slot table exhausted",
            );
            return null;
        };

        // Get current greenlet
        const current = py.py_helper_greenlet_getcurrent() orelse {
            self.op_slots.release(slot);
            self.fd_to_conn[fd_usize] = null;
            self.pool.release(conn);
            linuxClose(fd);
            return null;
        };

        // Store sockaddr in the slot — survives greenlet stack swaps.
        slot.storage = .{ .sockaddr = makeSockaddrIn4(addr_bytes, port) };

        // Submit connect SQE using op_slot user_data
        const user_data = op_slot.encodeUserData(slot.slot_index, slot.generation);
        _ = self.ring.prepConnect(fd, &slot.storage.sockaddr, @sizeOf(posix.sockaddr), user_data) catch {
            py.py_helper_decref(current);
            self.op_slots.release(slot);
            self.fd_to_conn[fd_usize] = null;
            self.pool.release(conn);
            linuxClose(fd);
            py.py_helper_err_set_string(
                py.py_helper_exc_oserror(),
                "green_connect: failed to submit connect SQE",
            );
            return null;
        };

        slot.greenlet = current;
        self.active_waits += 1;

        // Switch to hub
        const hub_g = self.hub_greenlet orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_connect: hub greenlet not set",
            );
            slot.greenlet = null;
            self.active_waits -= 1;
            py.py_helper_decref(current);
            self.op_slots.release(slot);
            self.fd_to_conn[fd_usize] = null;
            self.pool.release(conn);
            linuxClose(fd);
            return null;
        };
        const switch_result = py.py_helper_greenlet_switch(hub_g, null, null);
        if (switch_result == null) {
            self.cleanupSlotSwitch(slot);
            self.fd_to_conn[fd_usize] = null;
            self.pool.release(conn);
            linuxClose(fd);
            return null;
        }
        py.py_helper_decref(switch_result);

        // On resume: check result
        const res = slot.result;
        self.op_slots.release(slot);

        if (res < 0) {
            self.fd_to_conn[fd_usize] = null;
            self.pool.release(conn);
            linuxClose(fd);
            py.py_helper_err_set_from_errno(-res);
            return null;
        }

        return py.PyLong_FromLong(@as(c_long, @intCast(fd)));
    }

    // -----------------------------------------------------------------------
    // green_connect_fd: connect an already-registered fd cooperatively
    // -----------------------------------------------------------------------

    pub fn greenConnectFd(self: *Hub, fd: posix.fd_t, host: [*:0]const u8, port: u16) ?*PyObject {
        // Verify fd is registered in the pool
        _ = self.getConn(fd) orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_connect_fd: fd not registered",
            );
            return null;
        };

        // Parse host address
        const addr_bytes = parseIpv4(host) orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_oserror(),
                "green_connect_fd: invalid IPv4 address",
            );
            return null;
        };

        // Acquire op slot for the connect operation
        const slot = self.op_slots.acquire() orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_connect_fd: op slot table exhausted",
            );
            return null;
        };

        // Get current greenlet
        const current = py.py_helper_greenlet_getcurrent() orelse {
            self.op_slots.release(slot);
            return null;
        };

        // Store sockaddr in the slot — survives greenlet stack swaps.
        slot.storage = .{ .sockaddr = makeSockaddrIn4(addr_bytes, port) };

        // Submit connect SQE using op_slot user_data
        const user_data = op_slot.encodeUserData(slot.slot_index, slot.generation);
        _ = self.ring.prepConnect(fd, &slot.storage.sockaddr, @sizeOf(posix.sockaddr), user_data) catch {
            py.py_helper_decref(current);
            self.op_slots.release(slot);
            py.py_helper_err_set_string(
                py.py_helper_exc_oserror(),
                "green_connect_fd: failed to submit connect SQE",
            );
            return null;
        };

        slot.greenlet = current;
        self.active_waits += 1;

        // Switch to hub
        const hub_g = self.hub_greenlet orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_connect_fd: hub greenlet not set",
            );
            slot.greenlet = null;
            self.active_waits -= 1;
            py.py_helper_decref(current);
            self.op_slots.release(slot);
            return null;
        };
        const switch_result = py.py_helper_greenlet_switch(hub_g, null, null);
        if (switch_result == null) {
            self.cleanupSlotSwitch(slot);
            return null;
        }
        py.py_helper_decref(switch_result);

        // On resume: check result
        const res = slot.result;
        self.op_slots.release(slot);

        if (res < 0) {
            py.py_helper_err_set_from_errno(-res);
            return null;
        }

        const none = py.py_helper_none();
        py.py_helper_incref(none);
        return none;
    }

    // -----------------------------------------------------------------------
    // green_poll_fd: wait for fd readiness via IORING_OP_POLL_ADD
    // -----------------------------------------------------------------------

    pub fn greenPollFd(self: *Hub, fd: posix.fd_t, events: u32) ?*PyObject {
        // Acquire op slot
        const slot = self.op_slots.acquire() orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_poll_fd: op slot table exhausted",
            );
            return null;
        };

        // Get current greenlet
        const current = py.py_helper_greenlet_getcurrent() orelse {
            self.op_slots.release(slot);
            return null;
        };

        // Submit POLL_ADD SQE
        const user_data = op_slot.encodeUserData(slot.slot_index, slot.generation);
        _ = self.ring.prepPollAdd(fd, events, user_data) catch {
            py.py_helper_decref(current);
            self.op_slots.release(slot);
            py.py_helper_err_set_string(
                py.py_helper_exc_oserror(),
                "green_poll_fd: failed to submit poll SQE",
            );
            return null;
        };

        slot.greenlet = current;
        self.active_waits += 1;

        // Switch to hub
        const hub_g = self.hub_greenlet orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_poll_fd: hub greenlet not set",
            );
            slot.greenlet = null;
            self.active_waits -= 1;
            py.py_helper_decref(current);
            self.op_slots.release(slot);
            return null;
        };
        const switch_result = py.py_helper_greenlet_switch(hub_g, null, null);
        if (switch_result == null) {
            self.cleanupSlotSwitch(slot);
            return null;
        }
        py.py_helper_decref(switch_result);

        // On resume: check result
        const res = slot.result;
        self.op_slots.release(slot);

        if (res < 0) {
            py.py_helper_err_set_from_errno(-res);
            return null;
        }

        // Return revents as int
        return py.PyLong_FromLong(@as(c_long, @intCast(res)));
    }

    // -----------------------------------------------------------------------
    // green_register_fd: register an external fd in the connection pool
    // -----------------------------------------------------------------------

    pub fn greenRegisterFd(self: *Hub, fd: posix.fd_t) ?*PyObject {
        const fd_usize: usize = @intCast(fd);
        if (fd_usize >= MAX_FDS) {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_register_fd: fd too large",
            );
            return null;
        }

        // Check if already registered
        if (self.fd_to_conn[fd_usize] != null) {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_register_fd: fd already registered",
            );
            return null;
        }

        const conn = self.pool.acquire() orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_register_fd: connection pool exhausted",
            );
            return null;
        };
        conn.fd = fd;
        conn.state = .idle;
        self.fd_to_conn[fd_usize] = conn.pool_index;

        const none = py.py_helper_none();
        py.py_helper_incref(none);
        return none;
    }

    // -----------------------------------------------------------------------
    // green_unregister_fd: remove fd from connection pool without closing it
    // -----------------------------------------------------------------------

    pub fn greenUnregisterFd(self: *Hub, fd: posix.fd_t) ?*PyObject {
        const fd_usize: usize = @intCast(fd);
        if (fd_usize >= MAX_FDS) {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_unregister_fd: fd too large",
            );
            return null;
        }

        const pool_idx = self.fd_to_conn[fd_usize] orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_unregister_fd: fd not registered",
            );
            return null;
        };

        const conn = &self.pool.connections[pool_idx];
        self.fd_to_conn[fd_usize] = null;
        // Do NOT close the fd -- Python owns it
        // But we need to clean up the connection
        if (conn.greenlet) |g_opaque| {
            const g: *PyObject = @ptrCast(@alignCast(g_opaque));
            py.py_helper_decref(g);
            conn.greenlet = null;
        }
        // pool.release() calls conn.reset() which calls arena.reset()
        self.pool.release(conn);

        const none = py.py_helper_none();
        py.py_helper_incref(none);
        return none;
    }

    // -----------------------------------------------------------------------
    // green_poll_multi: wait for any of N fds to become ready, with timeout
    // -----------------------------------------------------------------------

    pub fn greenPollMulti(
        self: *Hub,
        fds: []const posix.fd_t,
        poll_events: []const u32,
        timeout_ms: i64,
    ) ?*PyObject {
        const n = fds.len;

        if (n == 0) {
            // No fds — just sleep for timeout or return empty
            if (timeout_ms > 0) {
                return self.greenSleep(@as(f64, @floatFromInt(timeout_ms)) / 1000.0);
            }
            return py.PyList_New(0);
        }

        const has_timeout = timeout_ms >= 0;
        const total_slots: usize = n + @as(usize, if (has_timeout) 1 else 0);

        // Acquire op slots
        var slots: [MAX_POLL_FDS + 1]*OpSlot = undefined;
        var acquired: usize = 0;
        for (0..total_slots) |i| {
            slots[i] = self.op_slots.acquire() orelse {
                // Release already-acquired
                for (0..acquired) |j| self.op_slots.release(slots[j]);
                py.py_helper_err_set_string(
                    py.py_helper_exc_runtime_error(),
                    "green_poll_multi: op slot table exhausted",
                );
                return null;
            };
            acquired = i + 1;
        }

        // Get current greenlet
        const current = py.py_helper_greenlet_getcurrent() orelse {
            for (0..total_slots) |j| self.op_slots.release(slots[j]);
            return null;
        };

        // Store PollGroup in the first slot's storage — heap-allocated,
        // survives greenlet stack swaps.  All slots reference it via pointer.
        slots[0].storage = .{ .poll_group = .{
            .resumed = false,
            .greenlet = current,
            .active_waits_n = @intCast(total_slots),
        } };
        const group_ptr = &slots[0].storage.poll_group;

        // Set poll_group on all slots
        for (0..total_slots) |i| {
            slots[i].poll_group = group_ptr;
        }

        // Submit POLL_ADD SQEs
        for (0..n) |i| {
            const user_data = op_slot.encodeUserData(slots[i].slot_index, slots[i].generation);
            _ = self.ring.prepPollAdd(fds[i], poll_events[i], user_data) catch {
                py.py_helper_decref(current);
                for (0..total_slots) |j| {
                    slots[j].poll_group = null;
                    self.op_slots.release(slots[j]);
                }
                py.py_helper_err_set_string(
                    py.py_helper_exc_oserror(),
                    "green_poll_multi: failed to submit poll SQE",
                );
                return null;
            };
        }

        // Submit TIMEOUT SQE if needed — store timespec in the timeout slot.
        if (has_timeout) {
            slots[n].storage = .{ .timespec = .{
                .sec = @divTrunc(timeout_ms, 1000),
                .nsec = @rem(timeout_ms, 1000) * 1_000_000,
            } };
            const timeout_ud = op_slot.encodeUserData(slots[n].slot_index, slots[n].generation);
            _ = self.ring.prepTimeout(&slots[n].storage.timespec, timeout_ud) catch {
                py.py_helper_decref(current);
                for (0..total_slots) |j| {
                    slots[j].poll_group = null;
                    self.op_slots.release(slots[j]);
                }
                py.py_helper_err_set_string(
                    py.py_helper_exc_oserror(),
                    "green_poll_multi: failed to submit timeout SQE",
                );
                return null;
            };
        }

        self.active_waits += total_slots;

        // Switch to hub
        const hub_g = self.hub_greenlet orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_poll_multi: hub greenlet not set",
            );
            self.active_waits -= total_slots;
            py.py_helper_decref(current);
            for (0..total_slots) |j| {
                slots[j].poll_group = null;
                self.op_slots.release(slots[j]);
            }
            return null;
        };
        const switch_result = py.py_helper_greenlet_switch(hub_g, null, null);
        if (switch_result == null) {
            // IMPORTANT: read group_ptr BEFORE cancelAndReleasePollSlots, which
            // releases slots[0] (where the PollGroup storage lives).
            // Do NOT decref the greenlet — this branch also runs during
            // dealloc switches (GreenletExit). See cleanupConnSwitch.
            if (!group_ptr.resumed) {
                self.active_waits -= total_slots;
            }
            self.cancelAndReleasePollSlots(slots[0..total_slots]);
            return null;
        }
        py.py_helper_decref(switch_result);

        // On resume: collect results from poll slots (not timeout slot)
        const result_list = py.PyList_New(0) orelse {
            self.cancelAndReleasePollSlots(slots[0..total_slots]);
            return null;
        };

        for (0..n) |i| {
            const res = slots[i].result;
            if (res > 0) {
                const tuple = py.py_helper_tuple_new(2) orelse {
                    py.py_helper_decref(result_list);
                    self.cancelAndReleasePollSlots(slots[0..total_slots]);
                    return null;
                };
                _ = py.py_helper_tuple_setitem(tuple, 0, py.PyLong_FromLong(@as(c_long, @intCast(fds[i]))));
                _ = py.py_helper_tuple_setitem(tuple, 1, py.PyLong_FromLong(@as(c_long, @intCast(res))));
                _ = py.PyList_Append(result_list, tuple);
                py.py_helper_decref(tuple);
            }
        }

        // Cancel remaining SQEs and release all slots
        self.cancelAndReleasePollSlots(slots[0..total_slots]);

        return result_list;
    }

    /// Cancel outstanding poll/timeout SQEs and release their op slots.
    fn cancelAndReleasePollSlots(self: *Hub, slots: []*OpSlot) void {
        for (slots) |slot| {
            const ud = op_slot.encodeUserData(slot.slot_index, slot.generation);
            _ = self.ring.prepCancelUserData(ud, IGNORE_SENTINEL) catch {};
        }
        _ = self.ring.submit() catch {};
        // Release after submitting cancels (generation bump invalidates stale CQEs)
        for (slots) |slot| {
            slot.poll_group = null;
            self.op_slots.release(slot);
        }
    }

    // -----------------------------------------------------------------------
    // green_poll_fd_timeout: single-fd poll with LINK_TIMEOUT
    // -----------------------------------------------------------------------

    pub fn greenPollFdTimeout(self: *Hub, fd: posix.fd_t, events: u32, timeout_ms: i64) ?*PyObject {
        // Acquire op slot
        const slot = self.op_slots.acquire() orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_poll_fd_timeout: op slot table exhausted",
            );
            return null;
        };

        // Get current greenlet
        const current = py.py_helper_greenlet_getcurrent() orelse {
            self.op_slots.release(slot);
            return null;
        };

        // Submit POLL_ADD SQE with IOSQE_IO_LINK flag
        const user_data = op_slot.encodeUserData(slot.slot_index, slot.generation);
        const sqe = self.ring.prepPollAdd(fd, events, user_data) catch {
            py.py_helper_decref(current);
            self.op_slots.release(slot);
            py.py_helper_err_set_string(
                py.py_helper_exc_oserror(),
                "green_poll_fd_timeout: failed to submit poll SQE",
            );
            return null;
        };
        sqe.flags |= IOSQE_IO_LINK;

        // Store timespec in the slot — survives greenlet stack swaps.
        slot.storage = .{ .timespec = .{
            .sec = @divTrunc(timeout_ms, 1000),
            .nsec = @rem(timeout_ms, 1000) * 1_000_000,
        } };
        _ = self.ring.prepLinkTimeout(&slot.storage.timespec, IGNORE_SENTINEL) catch {
            py.py_helper_decref(current);
            self.op_slots.release(slot);
            py.py_helper_err_set_string(
                py.py_helper_exc_oserror(),
                "green_poll_fd_timeout: failed to submit link_timeout SQE",
            );
            return null;
        };

        slot.greenlet = current;
        self.active_waits += 1;

        // Switch to hub
        const hub_g = self.hub_greenlet orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "green_poll_fd_timeout: hub greenlet not set",
            );
            slot.greenlet = null;
            self.active_waits -= 1;
            py.py_helper_decref(current);
            self.op_slots.release(slot);
            return null;
        };
        const switch_result = py.py_helper_greenlet_switch(hub_g, null, null);
        if (switch_result == null) {
            self.cleanupSlotSwitch(slot);
            return null;
        }
        py.py_helper_decref(switch_result);

        // On resume: check result
        const res = slot.result;
        self.op_slots.release(slot);

        if (res > 0) {
            // Poll completed — return revents
            return py.PyLong_FromLong(@as(c_long, @intCast(res)));
        }

        const ecanceled_neg = -@as(i32, @intFromEnum(linux.E.CANCELED));
        if (res == ecanceled_neg) {
            // Timeout fired — poll was cancelled. Return 0.
            return py.PyLong_FromLong(0);
        }

        if (res < 0) {
            py.py_helper_err_set_from_errno(-res);
            return null;
        }

        // res == 0 — shouldn't happen for poll, return 0
        return py.PyLong_FromLong(0);
    }
};

// ---------------------------------------------------------------------------
// IPv4 address parser (helper for greenConnect)
// ---------------------------------------------------------------------------

fn parseIpv4(host: [*:0]const u8) ?[4]u8 {
    var result: [4]u8 = undefined;
    var octet_idx: usize = 0;
    var current_val: u16 = 0;
    var digits: u8 = 0;
    var i: usize = 0;
    while (host[i] != 0) : (i += 1) {
        const c = host[i];
        if (c == '.') {
            if (digits == 0 or octet_idx >= 3) return null;
            if (current_val > 255) return null;
            result[octet_idx] = @intCast(current_val);
            octet_idx += 1;
            current_val = 0;
            digits = 0;
        } else if (c >= '0' and c <= '9') {
            current_val = current_val * 10 + (c - '0');
            digits += 1;
            if (digits > 3) return null;
        } else {
            return null;
        }
    }
    if (digits == 0 or octet_idx != 3) return null;
    if (current_val > 255) return null;
    result[octet_idx] = @intCast(current_val);
    return result;
}

// ---------------------------------------------------------------------------
// Python dict → ArenaConfig parser
// ---------------------------------------------------------------------------

/// Read an integer value from a Python dict by key. Returns the default
/// if the dict is null, the key is missing, or the value is not an int.
fn dictGetU32(dict: *PyObject, key: [*:0]const u8, default: u32) u32 {
    const item: ?*PyObject = py.PyDict_GetItemString(dict, key);
    if (item == null) return default;
    const val = py.PyLong_AsLong(item.?);
    if (val == -1 and py.py_helper_err_occurred() != null) {
        py.PyErr_Clear();
        return default;
    }
    if (val < 0) return default;
    if (val > std.math.maxInt(u32)) return default;
    return @intCast(val);
}

fn dictGetU16(dict: *PyObject, key: [*:0]const u8, default: u16) u16 {
    const item: ?*PyObject = py.PyDict_GetItemString(dict, key);
    if (item == null) return default;
    const val = py.PyLong_AsLong(item.?);
    if (val == -1 and py.py_helper_err_occurred() != null) {
        py.PyErr_Clear();
        return default;
    }
    if (val < 0) return default;
    if (val > std.math.maxInt(u16)) return default;
    return @intCast(val);
}

/// Parse an ArenaConfig from a Python dict. Missing keys use defaults.
/// If dict is null, returns all defaults.
fn configFromPyDict(dict: ?*PyObject) ArenaConfig {
    const d = ArenaConfig.defaults();
    const obj = dict orelse return d;
    return ArenaConfig{
        .chunk_size = dictGetU32(obj, "chunk_size", d.chunk_size),
        .max_header_size = dictGetU32(obj, "max_header_size", d.max_header_size),
        .max_body_size = dictGetU32(obj, "max_body_size", d.max_body_size),
        .max_connections = dictGetU16(obj, "max_connections", d.max_connections),
        .read_timeout_ms = dictGetU32(obj, "read_timeout_ms", d.read_timeout_ms),
        .keepalive_timeout_ms = dictGetU32(obj, "keepalive_timeout_ms", d.keepalive_timeout_ms),
        .handler_timeout_ms = dictGetU32(obj, "handler_timeout_ms", d.handler_timeout_ms),
    };
}

// ---------------------------------------------------------------------------
// Global hub instance (one per process)
// ---------------------------------------------------------------------------

var global_hub: ?*Hub = null;

fn getHub() ?*Hub {
    return global_hub;
}

// ---------------------------------------------------------------------------
// Python-facing API functions (called from extension.zig)
// ---------------------------------------------------------------------------

/// hub_run(listen_fd, acceptor_fn, config_dict=None) → None
pub fn pyHubRun(_: ?*PyObject, args: ?*PyObject) callconv(.c) ?*PyObject {
    var listen_fd_long: c_long = 0;
    var acceptor_fn: ?*PyObject = null;
    var config_dict: ?*PyObject = null;
    if (py.PyArg_ParseTuple(args, "lO|O", &listen_fd_long, &acceptor_fn, &config_dict) == 0)
        return null;

    const config = configFromPyDict(config_dict);
    const allocator = std.heap.c_allocator;

    // Allocate hub on the heap so it survives across greenlet switches
    const hub_ptr = allocator.create(Hub) catch {
        _ = py.py_helper_err_no_memory();
        return null;
    };
    hub_ptr.* = Hub.init(allocator, config) catch {
        allocator.destroy(hub_ptr);
        py.py_helper_err_set_string(
            py.py_helper_exc_runtime_error(),
            "hub_run: failed to initialize hub",
        );
        return null;
    };
    // Fix up recycler pointers: Hub.init() returns by value, so both
    // recyclers moved to hub_ptr. Arenas and InputBuffers still point
    // to the old (dead) stack address. Re-point them to the final heap address.
    hub_ptr.pool.fixupRecycler(&hub_ptr.recycler);
    hub_ptr.pool.fixupBufRecycler(&hub_ptr.buf_recycler);
    global_hub = hub_ptr;

    // The current greenlet IS the hub greenlet
    hub_ptr.hub_greenlet = py.py_helper_greenlet_getcurrent();
    if (hub_ptr.hub_greenlet == null) {
        global_hub = null;
        hub_ptr.deinit();
        allocator.destroy(hub_ptr);
        return null;
    }

    // Create the acceptor greenlet with hub as parent
    const acceptor_g = py.py_helper_greenlet_new(acceptor_fn, hub_ptr.hub_greenlet) orelse {
        global_hub = null;
        hub_ptr.deinit();
        allocator.destroy(hub_ptr);
        return null;
    };

    // Schedule the acceptor to run
    py.py_helper_incref(acceptor_g);
    hub_ptr.ready.append(hub_ptr.allocator, acceptor_g) catch {
        py.py_helper_decref(acceptor_g);
        py.py_helper_decref(acceptor_g);
        global_hub = null;
        hub_ptr.deinit();
        allocator.destroy(hub_ptr);
        _ = py.py_helper_err_no_memory();
        return null;
    };
    py.py_helper_decref(acceptor_g);

    // Run the hub loop
    hub_ptr.hubLoop();

    // Cleanup
    global_hub = null;
    hub_ptr.deinit();
    allocator.destroy(hub_ptr);

    if (py.py_helper_err_occurred() != null)
        return null;

    const none = py.py_helper_none();
    py.py_helper_incref(none);
    return none;
}

/// green_accept(listen_fd) → int
pub fn pyGreenAccept(_: ?*PyObject, args: ?*PyObject) callconv(.c) ?*PyObject {
    var listen_fd_long: c_long = 0;
    if (py.PyArg_ParseTuple(args, "l", &listen_fd_long) == 0)
        return null;

    const hub_ptr = getHub() orelse {
        py.py_helper_err_set_string(
            py.py_helper_exc_runtime_error(),
            "green_accept: hub not running",
        );
        return null;
    };

    const listen_fd: posix.fd_t = @intCast(listen_fd_long);
    return hub_ptr.greenAccept(listen_fd);
}

/// green_recv(fd, max_bytes) → bytes
pub fn pyGreenRecv(_: ?*PyObject, args: ?*PyObject) callconv(.c) ?*PyObject {
    var fd_long: c_long = 0;
    var max_bytes_long: c_long = 0;
    if (py.PyArg_ParseTuple(args, "ll", &fd_long, &max_bytes_long) == 0)
        return null;

    const hub_ptr = getHub() orelse {
        py.py_helper_err_set_string(
            py.py_helper_exc_runtime_error(),
            "green_recv: hub not running",
        );
        return null;
    };

    const fd: posix.fd_t = @intCast(fd_long);
    const max_bytes: usize = @intCast(max_bytes_long);
    return hub_ptr.greenRecv(fd, max_bytes);
}

/// green_send(fd, data) → int
pub fn pyGreenSend(_: ?*PyObject, args: ?*PyObject) callconv(.c) ?*PyObject {
    var fd_long: c_long = 0;
    var data: ?*PyObject = null;
    if (py.PyArg_ParseTuple(args, "lO", &fd_long, &data) == 0)
        return null;

    const hub_ptr = getHub() orelse {
        py.py_helper_err_set_string(
            py.py_helper_exc_runtime_error(),
            "green_send: hub not running",
        );
        return null;
    };

    const fd: posix.fd_t = @intCast(fd_long);
    return hub_ptr.greenSend(fd, data.?);
}

/// hub_stop() → None
pub fn pyHubStop(_: ?*PyObject, _: ?*PyObject) callconv(.c) ?*PyObject {
    const hub_ptr = getHub() orelse {
        py.py_helper_err_set_string(
            py.py_helper_exc_runtime_error(),
            "hub_stop: hub not running",
        );
        return null;
    };

    _ = linuxWrite(hub_ptr.stop_pipe[1], &[_]u8{1}) catch {
        py.py_helper_err_set_string(
            py.py_helper_exc_oserror(),
            "hub_stop: failed to write to stop pipe",
        );
        return null;
    };

    const none = py.py_helper_none();
    py.py_helper_incref(none);
    return none;
}

/// hub_schedule(greenlet) → None
pub fn pyHubSchedule(_: ?*PyObject, args: ?*PyObject) callconv(.c) ?*PyObject {
    var greenlet: ?*PyObject = null;
    if (py.PyArg_ParseTuple(args, "O", &greenlet) == 0)
        return null;

    const hub_ptr = getHub() orelse {
        py.py_helper_err_set_string(
            py.py_helper_exc_runtime_error(),
            "hub_schedule: hub not running",
        );
        return null;
    };

    const g = greenlet.?;
    py.py_helper_incref(g);
    hub_ptr.ready.append(hub_ptr.allocator, g) catch {
        py.py_helper_decref(g);
        _ = py.py_helper_err_no_memory();
        return null;
    };

    const none = py.py_helper_none();
    py.py_helper_incref(none);
    return none;
}

/// get_active_waits() → int
pub fn pyGetActiveWaits(_: ?*PyObject, _: ?*PyObject) callconv(.c) ?*PyObject {
    const hub_ptr = getHub() orelse {
        return py.PyLong_FromLong(0);
    };
    return py.PyLong_FromLong(@as(c_long, @intCast(hub_ptr.active_waits)));
}

/// get_hub_greenlet() → greenlet
pub fn pyGetHubGreenlet(_: ?*PyObject, _: ?*PyObject) callconv(.c) ?*PyObject {
    const hub_ptr = getHub() orelse {
        py.py_helper_err_set_string(
            py.py_helper_exc_runtime_error(),
            "get_hub_greenlet: hub not running",
        );
        return null;
    };

    const g = hub_ptr.hub_greenlet orelse {
        py.py_helper_err_set_string(
            py.py_helper_exc_runtime_error(),
            "get_hub_greenlet: hub greenlet not set",
        );
        return null;
    };

    py.py_helper_incref(g);
    return g;
}

/// green_close(fd) → None
pub fn pyGreenClose(_: ?*PyObject, args: ?*PyObject) callconv(.c) ?*PyObject {
    var fd_long: c_long = 0;
    if (py.PyArg_ParseTuple(args, "l", &fd_long) == 0)
        return null;

    const hub_ptr = getHub() orelse {
        // No hub running — just close the fd directly
        linuxClose(@intCast(fd_long));
        const none = py.py_helper_none();
        py.py_helper_incref(none);
        return none;
    };

    const fd: posix.fd_t = @intCast(fd_long);
    return hub_ptr.greenClose(fd);
}

/// green_sleep(seconds) → None
pub fn pyGreenSleep(_: ?*PyObject, args: ?*PyObject) callconv(.c) ?*PyObject {
    var seconds: f64 = 0;
    if (py.PyArg_ParseTuple(args, "d", &seconds) == 0)
        return null;

    const hub_ptr = getHub() orelse {
        py.py_helper_err_set_string(
            py.py_helper_exc_runtime_error(),
            "green_sleep: hub not running",
        );
        return null;
    };

    return hub_ptr.greenSleep(seconds);
}

/// green_connect(host, port) → int (fd)
pub fn pyGreenConnect(_: ?*PyObject, args: ?*PyObject) callconv(.c) ?*PyObject {
    var host: [*c]const u8 = null;
    var port_long: c_long = 0;
    if (py.PyArg_ParseTuple(args, "sl", &host, &port_long) == 0)
        return null;

    const hub_ptr = getHub() orelse {
        py.py_helper_err_set_string(
            py.py_helper_exc_runtime_error(),
            "green_connect: hub not running",
        );
        return null;
    };

    // Convert [*c]const u8 to [*:0]const u8 (PyArg_ParseTuple "s" guarantees null-terminated)
    const host_sentinel: [*:0]const u8 = @ptrCast(host);
    return hub_ptr.greenConnect(host_sentinel, @intCast(port_long));
}

/// green_connect_fd(fd, host, port) → None
pub fn pyGreenConnectFd(_: ?*PyObject, args: ?*PyObject) callconv(.c) ?*PyObject {
    var fd_long: c_long = 0;
    var host: [*c]const u8 = null;
    var port_long: c_long = 0;
    if (py.PyArg_ParseTuple(args, "lsl", &fd_long, &host, &port_long) == 0)
        return null;

    const hub_ptr = getHub() orelse {
        py.py_helper_err_set_string(
            py.py_helper_exc_runtime_error(),
            "green_connect_fd: hub not running",
        );
        return null;
    };

    const host_sentinel: [*:0]const u8 = @ptrCast(host);
    return hub_ptr.greenConnectFd(@intCast(fd_long), host_sentinel, @intCast(port_long));
}

/// green_poll_fd(fd, events) → int (revents)
pub fn pyGreenPollFd(_: ?*PyObject, args: ?*PyObject) callconv(.c) ?*PyObject {
    var fd_long: c_long = 0;
    var events_long: c_long = 0;
    if (py.PyArg_ParseTuple(args, "ll", &fd_long, &events_long) == 0)
        return null;

    const hub_ptr = getHub() orelse {
        py.py_helper_err_set_string(
            py.py_helper_exc_runtime_error(),
            "green_poll_fd: hub not running",
        );
        return null;
    };

    return hub_ptr.greenPollFd(@intCast(fd_long), @intCast(events_long));
}

/// green_register_fd(fd) → None
pub fn pyGreenRegisterFd(_: ?*PyObject, args: ?*PyObject) callconv(.c) ?*PyObject {
    var fd_long: c_long = 0;
    if (py.PyArg_ParseTuple(args, "l", &fd_long) == 0)
        return null;

    const hub_ptr = getHub() orelse {
        py.py_helper_err_set_string(
            py.py_helper_exc_runtime_error(),
            "green_register_fd: hub not running",
        );
        return null;
    };

    return hub_ptr.greenRegisterFd(@intCast(fd_long));
}

/// green_unregister_fd(fd) → None
pub fn pyGreenUnregisterFd(_: ?*PyObject, args: ?*PyObject) callconv(.c) ?*PyObject {
    var fd_long: c_long = 0;
    if (py.PyArg_ParseTuple(args, "l", &fd_long) == 0)
        return null;

    const hub_ptr = getHub() orelse {
        py.py_helper_err_set_string(
            py.py_helper_exc_runtime_error(),
            "green_unregister_fd: hub not running",
        );
        return null;
    };

    return hub_ptr.greenUnregisterFd(@intCast(fd_long));
}

/// green_poll_multi(fds_list, events_list, timeout_ms) → list[tuple[int, int]]
pub fn pyGreenPollMulti(_: ?*PyObject, args: ?*PyObject) callconv(.c) ?*PyObject {
    var fds_list: ?*PyObject = null;
    var events_list: ?*PyObject = null;
    var timeout_ms_long: c_long = 0;
    if (py.PyArg_ParseTuple(args, "OOl", &fds_list, &events_list, &timeout_ms_long) == 0)
        return null;

    const hub_ptr = getHub() orelse {
        py.py_helper_err_set_string(
            py.py_helper_exc_runtime_error(),
            "green_poll_multi: hub not running",
        );
        return null;
    };

    const n_raw = py.PyList_Size(fds_list.?);
    if (n_raw < 0) return null;
    const n: usize = @intCast(n_raw);

    if (n > MAX_POLL_FDS) {
        py.py_helper_err_set_string(
            py.py_helper_exc_runtime_error(),
            "green_poll_multi: too many fds (max 64)",
        );
        return null;
    }

    // Extract fds and events into stack arrays
    var fds: [MAX_POLL_FDS]posix.fd_t = undefined;
    var poll_events: [MAX_POLL_FDS]u32 = undefined;

    for (0..n) |i| {
        const fd_obj = py.PyList_GetItem(fds_list.?, @intCast(i)) orelse return null;
        const fd_val = py.PyLong_AsLong(fd_obj);
        if (fd_val == -1 and py.py_helper_err_occurred() != null) return null;
        fds[i] = @intCast(fd_val);

        const ev_obj = py.PyList_GetItem(events_list.?, @intCast(i)) orelse return null;
        const ev_val = py.PyLong_AsLong(ev_obj);
        if (ev_val == -1 and py.py_helper_err_occurred() != null) return null;
        poll_events[i] = @intCast(ev_val);
    }

    return hub_ptr.greenPollMulti(fds[0..n], poll_events[0..n], timeout_ms_long);
}

/// green_poll_fd_timeout(fd, events, timeout_ms) → int
pub fn pyGreenPollFdTimeout(_: ?*PyObject, args: ?*PyObject) callconv(.c) ?*PyObject {
    var fd_long: c_long = 0;
    var events_long: c_long = 0;
    var timeout_ms_long: c_long = 0;
    if (py.PyArg_ParseTuple(args, "lll", &fd_long, &events_long, &timeout_ms_long) == 0)
        return null;

    const hub_ptr = getHub() orelse {
        py.py_helper_err_set_string(
            py.py_helper_exc_runtime_error(),
            "green_poll_fd_timeout: hub not running",
        );
        return null;
    };

    return hub_ptr.greenPollFdTimeout(@intCast(fd_long), @intCast(events_long), timeout_ms_long);
}

// ---------------------------------------------------------------------------
// HTTP bridge functions
// ---------------------------------------------------------------------------

fn methodToStr(method: http.Method) []const u8 {
    return switch (method) {
        .get => "GET",
        .post => "POST",
        .head => "HEAD",
        .put => "PUT",
        .delete => "DELETE",
        .connect => "CONNECT",
        .options => "OPTIONS",
        .trace => "TRACE",
        .patch => "PATCH",
        .unknown => "UNKNOWN",
    };
}

/// Compute an upper bound on the formatted HTTP/1.1 response header size.
/// Mirrors h2o's flatten_headers_estimate_size(): headers only, not body.
/// Typical result is ~200-500 bytes -- always bump-allocated in the arena.
fn estimateHeaderSize(
    status: http_response.StatusCode,
    headers: []const http_response.Header,
    body_len: usize,
) usize {
    _ = body_len;
    // Status line: "HTTP/1.1 XXX {phrase}\r\n"
    var len: usize = "HTTP/1.1  \r\n".len + 3 + status.phrase().len;

    // Content-Length header: "Content-Length: {digits}\r\n"
    // 20 digits = worst-case for u64 (h2o uses sizeof(H2O_UINT64_LONGEST_STR))
    len += "Content-Length: \r\n".len + 20;

    // Final CRLF separating headers from body
    len += "\r\n".len;

    // User headers: sum actual name + value lengths + 4 bytes per header for ": \r\n"
    for (headers) |hdr| {
        len += hdr.name.len + hdr.value.len + 4;
    }

    return len;
}

fn statusFromInt(code: u16) ?http_response.StatusCode {
    return switch (code) {
        200 => .ok,
        201 => .created,
        204 => .no_content,
        301 => .moved_permanently,
        302 => .found,
        304 => .not_modified,
        400 => .bad_request,
        401 => .unauthorized,
        403 => .forbidden,
        404 => .not_found,
        405 => .method_not_allowed,
        413 => .payload_too_large,
        431 => .request_header_fields_too_large,
        500 => .internal_server_error,
        502 => .bad_gateway,
        503 => .service_unavailable,
        504 => .gateway_timeout,
        else => null,
    };
}

/// Extracts a `[]const u8` slice from a Python bytes object.
/// Returns the slice on success, or null if the object is not valid bytes.
fn extractBytesSlice(obj: *PyObject) ?[]const u8 {
    const ptr: [*]const u8 = @ptrCast(py.py_helper_bytes_as_string(obj) orelse return null);
    const len: usize = @intCast(py.py_helper_bytes_get_size(obj));
    return ptr[0..len];
}

/// http_send_response(fd, status, headers, body) -> int
///
/// Formats response headers into the per-connection arena, then sends
/// headers + body via writev. Zero-copy for the body.
pub fn pyHttpSendResponse(
    _: ?*PyObject,
    args: ?*PyObject,
) callconv(.c) ?*PyObject {
    var fd_long: c_long = 0;
    var status_long: c_long = 0;
    var headers_list: ?*PyObject = null;
    var body_obj: ?*PyObject = null;
    if (py.PyArg_ParseTuple(args, "llOS", &fd_long, &status_long, &headers_list, &body_obj) == 0)
        return null;

    const hub_ptr = getHub() orelse {
        py.py_helper_err_set_string(
            py.py_helper_exc_runtime_error(),
            "http_send_response: hub not running",
        );
        return null;
    };

    const fd: posix.fd_t = @intCast(fd_long);

    // Convert status code
    const status_code: u16 = @intCast(status_long);
    const status = statusFromInt(status_code) orelse {
        py.py_helper_err_set_string(
            py.py_helper_exc_value_error(),
            "Unsupported HTTP status code",
        );
        return null;
    };

    // Extract body
    const body_ptr: [*]const u8 = @ptrCast(py.py_helper_bytes_as_string(body_obj.?) orelse {
        py.py_helper_err_set_string(
            py.py_helper_exc_runtime_error(),
            "http_send_response: invalid body bytes",
        );
        return null;
    });
    const body_len: usize = @intCast(py.py_helper_bytes_get_size(body_obj.?));
    const body_slice = body_ptr[0..body_len];

    // Extract headers from Python list of (bytes, bytes) tuples
    const n_headers_raw = py.PyList_Size(headers_list.?);
    if (n_headers_raw < 0) return null;
    const n_headers: usize = @intCast(n_headers_raw);

    var stack_headers: [64]http_response.Header = undefined;
    var heap_headers: ?[]http_response.Header = null;
    defer if (heap_headers) |h| std.heap.c_allocator.free(h);

    const zig_headers: []http_response.Header = if (n_headers <= 64)
        stack_headers[0..n_headers]
    else blk: {
        heap_headers = std.heap.c_allocator.alloc(http_response.Header, n_headers) catch {
            _ = py.py_helper_err_no_memory();
            return null;
        };
        break :blk heap_headers.?;
    };

    for (0..n_headers) |i| {
        const pair = py.PyList_GetItem(headers_list.?, @intCast(i)) orelse return null;
        const name_obj = py.py_helper_tuple_getitem(pair, 0) orelse return null;
        const val_obj = py.py_helper_tuple_getitem(pair, 1) orelse return null;

        const name_slice = extractBytesSlice(name_obj) orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "http_send_response: invalid header name bytes",
            );
            return null;
        };
        const val_slice = extractBytesSlice(val_obj) orelse {
            py.py_helper_err_set_string(
                py.py_helper_exc_runtime_error(),
                "http_send_response: invalid header value bytes",
            );
            return null;
        };

        zig_headers[i] = .{
            .name = name_slice,
            .value = val_slice,
        };
    }

    return hub_ptr.greenSendResponse(fd, status, zig_headers, body_obj.?, body_slice);
}

// ---------------------------------------------------------------------------
// Phase 2: http_read_request — combined recv + accumulate + parse
// ---------------------------------------------------------------------------

/// Parsed result. `headers` is a slice into arena memory.
/// Valid until the arena is reset (end of request).
const ParsedRequest = struct {
    method: http.Method,
    path: []const u8, // zero-copy slice into InputBuffer
    version: http.Version,
    headers: []const http.Header, // arena-allocated, values point into InputBuffer
    body: []const u8, // zero-copy slice into InputBuffer
    raw_bytes_consumed: usize,
    keep_alive: bool,
    query_offset: usize, // offset of '?' in path, or path.len if none
};

const ParseResult = union(enum) {
    complete: ParsedRequest,
    incomplete,
    invalid,
    header_too_large,
    body_too_large,
    arena_oom,
};

/// Parse with arena-allocated headers. Fixed limit of MAX_HEADERS.
///
/// Like h2o: header structs live in the per-request arena (bump-allocated),
/// header name/value slices are zero-copy pointers into buf (the InputBuffer).
/// If more than MAX_HEADERS are present, the request is rejected.
fn tryParse(
    buf: []const u8,
    max_header_size: usize,
    max_body_size: usize,
    arena: *RequestArena,
) ParseResult {
    // Allocate header array from the arena (bump allocation, O(1))
    var headers = arena.allocSlice(http.Header, MAX_HEADERS) orelse return .arena_oom;

    var method: http.Method = .unknown;
    var path: ?[]const u8 = null;
    var version: http.Version = .@"1.0";
    var header_count: usize = 0;

    const header_bytes = hparse.parseRequest(
        buf,
        &method,
        &path,
        &version,
        headers,
        &header_count,
    ) catch |err| switch (err) {
        error.Incomplete => return .incomplete,
        error.Invalid => return .invalid,
    };

    // Validate limits.
    if (header_bytes > max_header_size) return .header_too_large;

    // Reject header keys that exceed the stack buffer size used by the headers
    // getter for lowercasing.  Without this check, a pathologically long header
    // key would overflow the getter's stack buffer.
    for (headers[0..header_count]) |hdr| {
        if (hdr.key.len > lazy_request.MAX_HEADER_KEY_LEN) return .invalid;
    }

    // Determine body length
    const content_length: usize = blk: {
        const cl = http.findHeader(headers[0..header_count], "content-length") orelse break :blk 0;
        const trimmed = std.mem.trim(u8, cl, &std.ascii.whitespace);
        break :blk std.fmt.parseInt(usize, trimmed, 10) catch return .invalid;
    };

    if (content_length > max_body_size) return .body_too_large;
    if (buf.len < header_bytes + content_length) return .incomplete;

    // Resolve keep-alive
    const ka = blk: {
        if (http.findHeader(headers[0..header_count], "connection")) |conn_hdr| {
            if (std.ascii.eqlIgnoreCase(conn_hdr, "close")) break :blk false;
            if (std.ascii.eqlIgnoreCase(conn_hdr, "keep-alive")) break :blk true;
        }
        break :blk version == .@"1.1";
    };

    const resolved_path = path orelse return .invalid;
    const query_offset = std.mem.indexOfScalar(u8, resolved_path, '?') orelse resolved_path.len;

    return .{
        .complete = .{
            .method = method,
            .path = resolved_path,
            .version = version,
            .headers = headers[0..header_count], // arena-owned slice
            .body = buf[header_bytes .. header_bytes + content_length],
            .raw_bytes_consumed = header_bytes + content_length,
            .keep_alive = ka,
            .query_offset = query_offset,
        },
    };
}

/// Build a LazyRequest Python object from a parsed request.
///
/// IMPORTANT: Must be called BEFORE conn.input.consume() — raw pointers
/// reference the InputBuffer's unconsumed region and arena-allocated headers.
/// After consume(), those bytes are logically freed and may be overwritten
/// by a future compact().
///
/// Safety: raw_headers_ptr points into arena-allocated memory (conn.arena),
/// and raw_body_ptr/raw_qs_ptr point into the InputBuffer's consumed region.
/// Both become invalid on the next pyHttpReadRequest() call: arena.reset()
/// frees the header array, and resetForNewRequest() → compact() may memmove
/// data over the consumed region. consume() after buildLazyRequest() only
/// advances read_pos (bytes stay in place), so pointers remain valid during
/// the handler. _invalidate() is called in the finally block before the next
/// pyHttpReadRequest() call.
fn buildLazyRequest(req: ParsedRequest) ?*PyObject {
    const request_type: *py.PyTypeObject = @ptrCast(&lazy_request.LazyRequestType);
    const obj = request_type.tp_alloc.?(request_type, 0) orelse return null;
    const self: *py.LazyRequestObject = @ptrCast(@alignCast(obj));

    // Initialize everything to null/zero so dealloc is safe on partial failure.
    self.method = null;
    self.path = null;
    self.full_path = null;
    self.cached_body = null;
    self.cached_headers = null;
    self.cached_query_params = null;
    self.cached_params = null;
    self.valid = 0;
    self.raw_body_ptr = null;
    self.raw_body_len = 0;
    self.raw_headers_ptr = null;
    self.raw_headers_count = 0;
    self.raw_qs_ptr = null;
    self.raw_qs_len = 0;
    self.keep_alive = 0;

    var success = false;
    defer if (!success) lazy_request.lazy_request_dealloc(@ptrCast(obj));

    // Eager: method (interned string, just incref)
    // @ptrCast needed: lazy_request.zig and hub.zig have separate @cImport namespaces,
    // producing structurally identical but nominally distinct PyObject types.
    self.method = @ptrCast(lazy_request.methodToPyStr(req.method) orelse return null);

    // Eager: path (portion before '?')
    self.path = py.PyUnicode_FromStringAndSize(
        req.path.ptr,
        @intCast(req.query_offset),
    ) orelse return null;

    // Eager: full_path
    if (req.query_offset < req.path.len) {
        self.full_path = py.PyUnicode_FromStringAndSize(
            req.path.ptr,
            @intCast(req.path.len),
        ) orelse return null;
        const qs_start = req.query_offset + 1;
        self.raw_qs_ptr = @ptrCast(req.path[qs_start..].ptr);
        self.raw_qs_len = @intCast(req.path.len - qs_start);
    } else {
        py.py_helper_incref(self.path.?);
        self.full_path = self.path; // same object, no query
        self.raw_qs_ptr = null;
        self.raw_qs_len = 0;
    }

    // Raw pointers (zero-copy)
    self.raw_body_ptr = @ptrCast(req.body.ptr);
    self.raw_body_len = @intCast(req.body.len);
    self.raw_headers_ptr = @ptrCast(@constCast(req.headers.ptr));
    self.raw_headers_count = @intCast(req.headers.len);

    self.keep_alive = @intFromBool(req.keep_alive);
    self.valid = 1;
    success = true;
    return obj;
}

/// Return Python None (with incref).
fn pyNone() ?*PyObject {
    const none = py.py_helper_none();
    py.py_helper_incref(none);
    return none;
}

fn raiseValueError() ?*PyObject {
    py.py_helper_err_set_string(
        py.py_helper_exc_value_error(),
        "Invalid HTTP request",
    );
    return null;
}

fn raiseHeaderTooLarge() ?*PyObject {
    py.py_helper_err_set_string(
        py.py_helper_exc_runtime_error(),
        "request header too large",
    );
    return null;
}

fn raiseBodyTooLarge() ?*PyObject {
    py.py_helper_err_set_string(
        py.py_helper_exc_runtime_error(),
        "request body too large",
    );
    return null;
}

fn raiseOutOfMemory() ?*PyObject {
    py.py_helper_err_set_string(
        py.py_helper_exc_runtime_error(),
        "out of memory",
    );
    return null;
}

fn raiseRequestTooLarge() ?*PyObject {
    py.py_helper_err_set_string(
        py.py_helper_exc_runtime_error(),
        "request too large",
    );
    return null;
}

/// http_read_request(fd, max_header_size, max_body_size) -> tuple | None
///
/// Combined recv + accumulate + parse in Zig. No Python-side accumulation.
/// Uses the connection's InputBuffer for zero-copy accumulation.
/// Returns structured request data to Python as
/// (method, path, body, keep_alive, headers) or None on EOF.
///
/// Raises:
///   ValueError: Malformed HTTP (400 Bad Request).
///   RuntimeError: Request too large (413 / 431) or out of memory.
///   OSError: Network error.
pub fn pyHttpReadRequest(
    _: ?*PyObject,
    args: ?*PyObject,
) callconv(.c) ?*PyObject {
    var fd_long: c_long = 0;
    var max_header_size_long: c_long = 0;
    var max_body_size_long: c_long = 0;
    if (py.PyArg_ParseTuple(
        args,
        "lll",
        &fd_long,
        &max_header_size_long,
        &max_body_size_long,
    ) == 0) return null;

    const hub_ptr = getHub() orelse {
        py.py_helper_err_set_string(
            py.py_helper_exc_runtime_error(),
            "http_read_request: hub not running",
        );
        return null;
    };

    const fd: posix.fd_t = @intCast(fd_long);
    const max_header_size: usize = @intCast(max_header_size_long);
    const max_body_size: usize = @intCast(max_body_size_long);

    const conn = hub_ptr.getConn(fd) orelse {
        py.py_helper_err_set_string(
            py.py_helper_exc_oserror(),
            "http_read_request: no connection for fd",
        );
        return null;
    };

    // -- STEP 0: Reset arena + compact leftover data --
    conn.arena.reset(); // return arena chunks to recycler
    conn.input.resetForNewRequest();

    // -- STEP 1: Try parsing from leftover data (pipelining) --
    if (conn.input.unconsumedLen() > 0) {
        const result = tryParse(
            conn.input.unconsumed(),
            max_header_size,
            max_body_size,
            &conn.arena,
        );
        switch (result) {
            .complete => |req| {
                // Build Python objects BEFORE consume -- header slices
                // point into InputBuffer's unconsumed region
                const py_result = buildLazyRequest(req) orelse return null;
                conn.input.consume(req.raw_bytes_consumed);
                return py_result;
            },
            .incomplete => {}, // fall through to recv
            .invalid => return raiseValueError(),
            .header_too_large => return raiseHeaderTooLarge(),
            .body_too_large => return raiseBodyTooLarge(),
            .arena_oom => return raiseOutOfMemory(),
        }
    }

    // -- STEP 2: Recv loop until complete request --
    while (true) {
        // Ensure space for recv
        const free = conn.input.reserveFree(RECV_SIZE) catch {
            return raiseOutOfMemory();
        };
        const recv_len = @min(free.len, RECV_SIZE);

        // Submit recv SQE, suspend greenlet, resume on completion
        const nbytes = hub_ptr.doRecv(fd, conn, free[0..recv_len]) orelse {
            return null; // error already set
        };

        if (nbytes == 0) {
            // EOF: client closed connection
            if (conn.input.unconsumedLen() > 0) {
                // Partial request in buffer -- malformed
                return raiseValueError(); // 400
            }
            // Clean close (no pending data)
            return pyNone();
        }

        conn.input.commitWrite(nbytes);

        // Hard limit check: total accumulated data
        if (conn.input.unconsumedLen() > max_header_size + max_body_size) {
            return raiseRequestTooLarge();
        }

        // Reset arena before re-parsing (discard any partial parse allocations)
        conn.arena.reset();

        // Try parsing -- headers allocated from arena
        const result = tryParse(
            conn.input.unconsumed(),
            max_header_size,
            max_body_size,
            &conn.arena,
        );
        switch (result) {
            .complete => |req| {
                // Build Python objects BEFORE consume -- header slices
                // point into InputBuffer's unconsumed region
                const py_result = buildLazyRequest(req) orelse return null;
                conn.input.consume(req.raw_bytes_consumed);
                return py_result;
            },
            .incomplete => continue, // recv more
            .invalid => return raiseValueError(),
            .header_too_large => return raiseHeaderTooLarge(),
            .body_too_large => return raiseBodyTooLarge(),
            .arena_oom => return raiseOutOfMemory(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests for tryParse
// ---------------------------------------------------------------------------

test "tryParse: simple GET request" {
    const allocator = std.testing.allocator;
    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    const first_chunk = try allocator.create(chunk_pool.Chunk);
    defer allocator.destroy(first_chunk);

    const config = ArenaConfig.defaults();
    var arena = RequestArena.init(first_chunk, &recycler, config.directThreshold());
    defer arena.reset();

    const buf = "GET /hello HTTP/1.1\r\nHost: localhost\r\n\r\n";
    const result = tryParse(buf, 32768, 1_048_576, &arena);
    switch (result) {
        .complete => |req| {
            try std.testing.expectEqual(http.Method.get, req.method);
            try std.testing.expectEqualStrings("/hello", req.path);
            try std.testing.expectEqual(http.Version.@"1.1", req.version);
            try std.testing.expectEqual(@as(usize, 1), req.headers.len);
            try std.testing.expectEqualStrings("Host", req.headers[0].key);
            try std.testing.expectEqualStrings("localhost", req.headers[0].value);
            try std.testing.expectEqualStrings("", req.body);
            try std.testing.expectEqual(buf.len, req.raw_bytes_consumed);
            try std.testing.expect(req.keep_alive);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "tryParse: POST with body" {
    const allocator = std.testing.allocator;
    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    const first_chunk = try allocator.create(chunk_pool.Chunk);
    defer allocator.destroy(first_chunk);

    const config = ArenaConfig.defaults();
    var arena = RequestArena.init(first_chunk, &recycler, config.directThreshold());
    defer arena.reset();

    const buf = "POST /submit HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello";
    const result = tryParse(buf, 32768, 1_048_576, &arena);
    switch (result) {
        .complete => |req| {
            try std.testing.expectEqual(http.Method.post, req.method);
            try std.testing.expectEqualStrings("/submit", req.path);
            try std.testing.expectEqualStrings("hello", req.body);
            try std.testing.expectEqual(buf.len, req.raw_bytes_consumed);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "tryParse: incomplete request" {
    const allocator = std.testing.allocator;
    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    const first_chunk = try allocator.create(chunk_pool.Chunk);
    defer allocator.destroy(first_chunk);

    const config = ArenaConfig.defaults();
    var arena = RequestArena.init(first_chunk, &recycler, config.directThreshold());
    defer arena.reset();

    const buf = "GET /path HTTP/1.1\r\nHost: local";
    const result = tryParse(buf, 32768, 1_048_576, &arena);
    try std.testing.expectEqual(ParseResult.incomplete, result);
}

test "tryParse: header_too_large" {
    const allocator = std.testing.allocator;
    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    const first_chunk = try allocator.create(chunk_pool.Chunk);
    defer allocator.destroy(first_chunk);

    const config = ArenaConfig.defaults();
    var arena = RequestArena.init(first_chunk, &recycler, config.directThreshold());
    defer arena.reset();

    // A simple request with small headers, but we set max_header_size very low
    const buf = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n";
    const result = tryParse(buf, 10, 1_048_576, &arena); // max_header_size=10
    try std.testing.expectEqual(ParseResult.header_too_large, result);
}

test "tryParse: body_too_large" {
    const allocator = std.testing.allocator;
    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    const first_chunk = try allocator.create(chunk_pool.Chunk);
    defer allocator.destroy(first_chunk);

    const config = ArenaConfig.defaults();
    var arena = RequestArena.init(first_chunk, &recycler, config.directThreshold());
    defer arena.reset();

    const buf = "POST / HTTP/1.1\r\nContent-Length: 100\r\n\r\n" ++ "x" ** 100;
    const result = tryParse(buf, 32768, 50, &arena); // max_body_size=50
    try std.testing.expectEqual(ParseResult.body_too_large, result);
}

test "tryParse: zero-length body (no Content-Length)" {
    const allocator = std.testing.allocator;
    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    const first_chunk = try allocator.create(chunk_pool.Chunk);
    defer allocator.destroy(first_chunk);

    const config = ArenaConfig.defaults();
    var arena = RequestArena.init(first_chunk, &recycler, config.directThreshold());
    defer arena.reset();

    const buf = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n";
    const result = tryParse(buf, 32768, 1_048_576, &arena);
    switch (result) {
        .complete => |req| {
            try std.testing.expectEqualStrings("", req.body);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "tryParse: Content-Length: 0" {
    const allocator = std.testing.allocator;
    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    const first_chunk = try allocator.create(chunk_pool.Chunk);
    defer allocator.destroy(first_chunk);

    const config = ArenaConfig.defaults();
    var arena = RequestArena.init(first_chunk, &recycler, config.directThreshold());
    defer arena.reset();

    const buf = "POST / HTTP/1.1\r\nContent-Length: 0\r\n\r\n";
    const result = tryParse(buf, 32768, 1_048_576, &arena);
    switch (result) {
        .complete => |req| {
            try std.testing.expectEqualStrings("", req.body);
            try std.testing.expectEqual(@as(usize, 0), req.body.len);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "tryParse: invalid Content-Length" {
    const allocator = std.testing.allocator;
    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    const first_chunk = try allocator.create(chunk_pool.Chunk);
    defer allocator.destroy(first_chunk);

    const config = ArenaConfig.defaults();
    var arena = RequestArena.init(first_chunk, &recycler, config.directThreshold());
    defer arena.reset();

    const buf = "POST / HTTP/1.1\r\nContent-Length: abc\r\n\r\n";
    const result = tryParse(buf, 32768, 1_048_576, &arena);
    try std.testing.expectEqual(ParseResult.invalid, result);
}

test "tryParse: duplicate headers preserved" {
    const allocator = std.testing.allocator;
    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    const first_chunk = try allocator.create(chunk_pool.Chunk);
    defer allocator.destroy(first_chunk);

    const config = ArenaConfig.defaults();
    var arena = RequestArena.init(first_chunk, &recycler, config.directThreshold());
    defer arena.reset();

    const buf = "GET / HTTP/1.1\r\nHost: localhost\r\nSet-Cookie: a=1\r\nSet-Cookie: b=2\r\n\r\n";
    const result = tryParse(buf, 32768, 1_048_576, &arena);
    switch (result) {
        .complete => |req| {
            try std.testing.expectEqual(@as(usize, 3), req.headers.len);
            try std.testing.expectEqualStrings("Host", req.headers[0].key);
            try std.testing.expectEqualStrings("Set-Cookie", req.headers[1].key);
            try std.testing.expectEqualStrings("a=1", req.headers[1].value);
            try std.testing.expectEqualStrings("Set-Cookie", req.headers[2].key);
            try std.testing.expectEqualStrings("b=2", req.headers[2].value);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "tryParse: Connection close means not keep-alive" {
    const allocator = std.testing.allocator;
    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    const first_chunk = try allocator.create(chunk_pool.Chunk);
    defer allocator.destroy(first_chunk);

    const config = ArenaConfig.defaults();
    var arena = RequestArena.init(first_chunk, &recycler, config.directThreshold());
    defer arena.reset();

    const buf = "GET / HTTP/1.1\r\nConnection: close\r\n\r\n";
    const result = tryParse(buf, 32768, 1_048_576, &arena);
    switch (result) {
        .complete => |req| {
            try std.testing.expect(!req.keep_alive);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "tryParse: HTTP/1.0 default not keep-alive" {
    const allocator = std.testing.allocator;
    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    const first_chunk = try allocator.create(chunk_pool.Chunk);
    defer allocator.destroy(first_chunk);

    const config = ArenaConfig.defaults();
    var arena = RequestArena.init(first_chunk, &recycler, config.directThreshold());
    defer arena.reset();

    const buf = "GET / HTTP/1.0\r\nHost: localhost\r\n\r\n";
    const result = tryParse(buf, 32768, 1_048_576, &arena);
    switch (result) {
        .complete => |req| {
            try std.testing.expect(!req.keep_alive);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "tryParse: incomplete body (Content-Length present but body not fully received)" {
    const allocator = std.testing.allocator;
    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    const first_chunk = try allocator.create(chunk_pool.Chunk);
    defer allocator.destroy(first_chunk);

    const config = ArenaConfig.defaults();
    var arena = RequestArena.init(first_chunk, &recycler, config.directThreshold());
    defer arena.reset();

    const buf = "POST / HTTP/1.1\r\nContent-Length: 10\r\n\r\nhello";
    const result = tryParse(buf, 32768, 1_048_576, &arena);
    try std.testing.expectEqual(ParseResult.incomplete, result);
}

test "tryParse: pipelined requests" {
    const allocator = std.testing.allocator;
    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    const first_chunk = try allocator.create(chunk_pool.Chunk);
    defer allocator.destroy(first_chunk);

    const config = ArenaConfig.defaults();
    var arena = RequestArena.init(first_chunk, &recycler, config.directThreshold());

    const buf = "GET /a HTTP/1.1\r\nHost: x\r\n\r\nGET /b HTTP/1.1\r\nHost: x\r\n\r\n";

    // Parse first request
    const result1 = tryParse(buf, 32768, 1_048_576, &arena);
    switch (result1) {
        .complete => |req| {
            try std.testing.expectEqualStrings("/a", req.path);
            // Parse second request from remaining bytes
            arena.reset();
            const remaining = buf[req.raw_bytes_consumed..];
            const result2 = tryParse(remaining, 32768, 1_048_576, &arena);
            switch (result2) {
                .complete => |req2| {
                    try std.testing.expectEqualStrings("/b", req2.path);
                },
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }
    arena.reset();
}

test "allocSlice: basic typed allocation" {
    const allocator = std.testing.allocator;
    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    const first_chunk = try allocator.create(chunk_pool.Chunk);
    defer allocator.destroy(first_chunk);

    const config = ArenaConfig.defaults();
    var arena = RequestArena.init(first_chunk, &recycler, config.directThreshold());
    defer arena.reset();

    const headers = arena.allocSlice(http.Header, 32) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 32), headers.len);

    // Write to verify memory is valid
    headers[0] = .{ .key = "Host", .value = "localhost" };
    headers[31] = .{ .key = "X-Last", .value = "yes" };
    try std.testing.expectEqualStrings("Host", headers[0].key);
    try std.testing.expectEqualStrings("X-Last", headers[31].key);
}

// ---------------------------------------------------------------------------
// estimateHeaderSize tests
// ---------------------------------------------------------------------------

test "header estimate covers small response" {
    const headers = [_]http_response.Header{
        .{ .name = "Content-Type", .value = "text/plain" },
    };
    const est = estimateHeaderSize(.ok, &headers, 5);
    var buf: [4096]u8 = undefined;
    const actual = try http_response.writeResponseHead(&buf, .ok, &headers, 5);
    try std.testing.expect(est >= actual);
}

test "header estimate covers large JWT header" {
    const jwt = "eyJ" ++ "x" ** 500 ++ "abc";
    const headers = [_]http_response.Header{
        .{ .name = "Authorization", .value = jwt },
    };
    const est = estimateHeaderSize(.ok, &headers, 2);
    var buf: [4096]u8 = undefined;
    const actual = try http_response.writeResponseHead(&buf, .ok, &headers, 2);
    try std.testing.expect(est >= actual);
}

test "header estimate covers empty response" {
    const est = estimateHeaderSize(.no_content, &.{}, 0);
    var buf: [4096]u8 = undefined;
    const actual = try http_response.writeResponseHead(&buf, .no_content, &.{}, 0);
    try std.testing.expect(est >= actual);
}

test "header estimate covers many headers" {
    var headers: [32]http_response.Header = undefined;
    for (&headers) |*h| {
        h.* = .{ .name = "X-Custom-Header", .value = "some-value" };
    }
    const est = estimateHeaderSize(.ok, &headers, 4);
    var buf: [4096]u8 = undefined;
    const actual = try http_response.writeResponseHead(&buf, .ok, &headers, 4);
    try std.testing.expect(est >= actual);
}
