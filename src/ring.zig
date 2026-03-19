const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;

pub const Ring = struct {
    io: linux.IoUring,

    pub fn init(queue_depth: u16) !Ring {
        const io = try linux.IoUring.init(queue_depth, 0);
        return Ring{ .io = io };
    }

    pub fn deinit(self: *Ring) void {
        self.io.deinit();
    }

    /// Queue an accept operation on a listening socket.
    pub fn prepAccept(self: *Ring, listen_fd: posix.fd_t, user_data: u64) !*linux.io_uring_sqe {
        return try self.io.accept(user_data, listen_fd, null, null, 0);
    }

    /// Queue a recv operation on a connected socket.
    pub fn prepRecv(self: *Ring, fd: posix.fd_t, buf: []u8, user_data: u64) !*linux.io_uring_sqe {
        return try self.io.recv(user_data, fd, .{ .buffer = buf }, 0);
    }

    /// Queue a send operation on a connected socket.
    pub fn prepSend(self: *Ring, fd: posix.fd_t, buf: []const u8, user_data: u64) !*linux.io_uring_sqe {
        return try self.io.send(user_data, fd, buf, 0);
    }

    /// Queue a writev (scatter/gather write) operation on a file descriptor.
    pub fn prepWritev(
        self: *Ring,
        fd: posix.fd_t,
        iovecs: []const posix.iovec_const,
        user_data: u64,
    ) !*linux.io_uring_sqe {
        return try self.io.writev(user_data, fd, iovecs, 0);
    }

    /// Queue a close operation on a file descriptor.
    pub fn prepClose(self: *Ring, fd: posix.fd_t, user_data: u64) !*linux.io_uring_sqe {
        return try self.io.close(user_data, fd);
    }

    /// Queue a multishot accept operation on a listening socket.
    /// A single SQE will produce multiple CQEs — one per accepted connection.
    /// Check IORING_CQE_F_MORE on each CQE: if set, more completions will follow.
    /// If not set, the multishot accept has been terminated and must be resubmitted.
    pub fn prepAcceptMultishot(self: *Ring, listen_fd: posix.fd_t, user_data: u64) !*linux.io_uring_sqe {
        return try self.io.accept_multishot(user_data, listen_fd, null, null, 0);
    }

    /// Initialise a provided buffer ring (BufferGroup) for kernel-managed buffer selection.
    /// The returned BufferGroup has its own recv, get, and put methods.
    pub fn initBufferGroup(self: *Ring, allocator: std.mem.Allocator, group_id: u16, buffer_size: u32, buffers_count: u16) !linux.IoUring.BufferGroup {
        return try linux.IoUring.BufferGroup.init(&self.io, allocator, group_id, buffer_size, buffers_count);
    }

    /// Queue a cancel operation for a previously submitted SQE identified by its user_data.
    pub fn prepCancel(self: *Ring, cancel_user_data: u64) !*linux.io_uring_sqe {
        return try self.io.cancel(0, cancel_user_data, 0);
    }

    /// Queue a read operation on a file descriptor (e.g. pipe).
    pub fn prepRead(self: *Ring, fd: posix.fd_t, buf: []u8, user_data: u64) !*linux.io_uring_sqe {
        return try self.io.read(user_data, fd, .{ .buffer = buf }, 0);
    }

    /// Queue a timeout operation. The kernel will complete this SQE after
    /// the specified timespec elapses, returning -ETIME.
    /// `count` is the number of completions to wait for before the timeout
    /// fires (0 means a pure timer — only the timespec matters).
    pub fn prepTimeout(self: *Ring, ts: *const linux.kernel_timespec, user_data: u64) !*linux.io_uring_sqe {
        return try self.io.timeout(user_data, ts, 0, 0);
    }

    /// Queue a connect operation on a socket fd.
    /// NOTE: We bypass IoUring.connect() because in Zig 0.15.2 it accepts
    /// posix.sockaddr but prep_connect expects linux.sockaddr — these diverge
    /// when libc is linked.
    pub fn prepConnect(self: *Ring, fd: posix.fd_t, addr: *const posix.sockaddr, addrlen: posix.socklen_t, user_data: u64) !*linux.io_uring_sqe {
        const sqe = try self.io.get_sqe();
        sqe.prep_connect(fd, @ptrCast(addr), addrlen);
        sqe.user_data = user_data;
        return sqe;
    }

    /// Queue a poll operation on a file descriptor.
    /// Returns the events that fired in the CQE result.
    pub fn prepPollAdd(self: *Ring, fd: posix.fd_t, poll_mask: u32, user_data: u64) !*linux.io_uring_sqe {
        return try self.io.poll_add(user_data, fd, poll_mask);
    }

    /// Queue a linked timeout that cancels the PREVIOUS SQE if the timeout
    /// fires first.  Must be submitted immediately after the SQE it guards,
    /// and the guarded SQE must have `IOSQE_IO_LINK` set in its flags.
    pub fn prepLinkTimeout(self: *Ring, ts: *const linux.kernel_timespec, user_data: u64) !*linux.io_uring_sqe {
        return try self.io.link_timeout(user_data, ts, 0);
    }

    /// Queue a cancel operation with explicit user_data for the cancel CQE.
    pub fn prepCancelUserData(self: *Ring, target_user_data: u64, own_user_data: u64) !*linux.io_uring_sqe {
        return try self.io.cancel(own_user_data, target_user_data, 0);
    }

    /// Submit all pending SQEs to the kernel.
    pub fn submit(self: *Ring) !u32 {
        return try self.io.submit();
    }

    /// Submit pending SQEs and wait for at least `wait_nr` completions.
    pub fn submitAndWait(self: *Ring, wait_nr: u32) !u32 {
        return try self.io.submit_and_wait(wait_nr);
    }

    /// Batch-drain CQEs into the caller's buffer. Returns a slice of the filled portion.
    /// `wait_nr` = 0 means non-blocking (return whatever is ready).
    pub fn copyCqes(self: *Ring, cqes_buf: []linux.io_uring_cqe, wait_nr: u32) ![]linux.io_uring_cqe {
        const count = try self.io.copy_cqes(cqes_buf, wait_nr);
        return cqes_buf[0..count];
    }

    /// Wait for and return one CQE. Submits pending SQEs and waits for at least one completion.
    pub fn waitCqe(self: *Ring) !linux.io_uring_cqe {
        _ = try self.io.submit_and_wait(1);
        return try self.io.copy_cqe();
    }
};

// ---------------------------------------------------------------------------
// Tests – Linux syscall helpers (posix wrappers removed in Zig 0.16)
// ---------------------------------------------------------------------------

fn testSocket() !posix.fd_t {
    const rc = linux.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    return if (posix.errno(rc) != .SUCCESS) error.SocketFailed else @intCast(rc);
}

fn testClose(fd: posix.fd_t) void {
    _ = linux.close(fd);
}

fn testSetsockopt(fd: posix.fd_t, optname: u32, optval: *const [4]u8) !void {
    const rc = linux.setsockopt(fd, posix.SOL.SOCKET, optname, @ptrCast(optval), 4);
    if (posix.errno(rc) != .SUCCESS) return error.SetsockoptFailed;
}

fn testBind(fd: posix.fd_t, addr: *const posix.sockaddr, addrlen: posix.socklen_t) !void {
    const rc = linux.bind(fd, addr, addrlen);
    if (posix.errno(rc) != .SUCCESS) return error.BindFailed;
}

fn testListen(fd: posix.fd_t, backlog: u31) !void {
    const rc = linux.listen(fd, backlog);
    if (posix.errno(rc) != .SUCCESS) return error.ListenFailed;
}

fn testConnect(fd: posix.fd_t, addr: *const posix.sockaddr, addrlen: posix.socklen_t) !void {
    const rc = linux.connect(fd, addr, addrlen);
    if (posix.errno(rc) != .SUCCESS) return error.ConnectFailed;
}

fn testGetsockname(fd: posix.fd_t, addr: *posix.sockaddr, addrlen: *posix.socklen_t) !void {
    const rc = linux.getsockname(fd, addr, addrlen);
    if (posix.errno(rc) != .SUCCESS) return error.GetsocknameFailed;
}

fn testWrite(fd: posix.fd_t, buf: []const u8) !usize {
    const rc = linux.write(fd, buf.ptr, buf.len);
    return if (posix.errno(rc) != .SUCCESS) error.WriteFailed else rc;
}

fn testRead(fd: posix.fd_t, buf: []u8) !usize {
    const rc = linux.read(fd, buf.ptr, buf.len);
    return if (posix.errno(rc) != .SUCCESS) error.ReadFailed else rc;
}

fn createListenSocket() !posix.fd_t {
    const fd = try testSocket();
    errdefer testClose(fd);

    // Allow port reuse
    const one: [4]u8 = @bitCast(@as(i32, 1));
    try testSetsockopt(fd, posix.SO.REUSEADDR, &one);

    const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 0); // port 0 = ephemeral
    try testBind(fd, &addr.any, addr.getOsSockLen());
    try testListen(fd, 1);
    return fd;
}

fn getBoundPort(fd: posix.fd_t) !u16 {
    var addr: posix.sockaddr.in = undefined;
    var addrlen: posix.socklen_t = @sizeOf(posix.sockaddr.in);
    try testGetsockname(fd, @ptrCast(&addr), &addrlen);
    return std.mem.bigToNative(u16, addr.port);
}

fn createClientSocket(port: u16) !posix.fd_t {
    const fd = try testSocket();
    errdefer testClose(fd);

    const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, port);
    try testConnect(fd, &addr.any, addr.getOsSockLen());
    return fd;
}

test "Ring init/deinit" {
    var ring = try Ring.init(16);
    defer ring.deinit();
}

test "Ring full accept-recv-send-close cycle" {
    var ring = try Ring.init(16);
    defer ring.deinit();

    // Create a TCP listen socket on an ephemeral port
    const listen_fd = try createListenSocket();
    defer testClose(listen_fd);

    const port = try getBoundPort(listen_fd);

    // Create a client socket and connect
    const client_fd = try createClientSocket(port);
    defer testClose(client_fd);

    // 1) accept via io_uring
    _ = try ring.prepAccept(listen_fd, 1);
    var cqe = try ring.waitCqe();
    try std.testing.expect(cqe.res >= 0);
    const accepted_fd: posix.fd_t = @intCast(cqe.res);
    // NOTE: We do NOT defer testClose(accepted_fd) here because step 6 closes it via io_uring.

    // 2) Client sends data (regular posix write)
    const msg = "hello io_uring";
    _ = try testWrite(client_fd, msg);

    // 3) recv via io_uring on the accepted fd
    var recv_buf: [64]u8 = undefined;
    _ = try ring.prepRecv(accepted_fd, &recv_buf, 2);
    cqe = try ring.waitCqe();
    try std.testing.expect(cqe.res > 0);
    const recv_len: usize = @intCast(cqe.res);
    try std.testing.expectEqualStrings(msg, recv_buf[0..recv_len]);

    // 4) send the data back via io_uring (echo)
    _ = try ring.prepSend(accepted_fd, recv_buf[0..recv_len], 3);
    cqe = try ring.waitCqe();
    try std.testing.expect(cqe.res > 0);

    // 5) Client reads the echoed data (regular posix read)
    var echo_buf: [64]u8 = undefined;
    const echo_len = try testRead(client_fd, &echo_buf);
    try std.testing.expectEqualStrings(msg, echo_buf[0..echo_len]);

    // 6) close the accepted fd via io_uring
    _ = try ring.prepClose(accepted_fd, 4);
    cqe = try ring.waitCqe();
    try std.testing.expect(cqe.res >= 0);
}

test "Ring multishot accept accepts multiple connections from one SQE" {
    var ring = try Ring.init(16);
    defer ring.deinit();

    // Create a listen socket with a large enough backlog for our test
    const listen_fd = try createListenSocket();
    defer testClose(listen_fd);

    // Re-set backlog to 16 (createListenSocket uses 1)
    try testListen(listen_fd, 16);

    const port = try getBoundPort(listen_fd);

    // Submit ONE multishot accept, then flush it to the kernel
    _ = try ring.prepAcceptMultishot(listen_fd, 42);
    _ = try ring.submit();

    const num_clients = 5;
    var client_fds: [num_clients]posix.socket_t = undefined;
    var accepted_fds: [num_clients]posix.fd_t = undefined;
    var accepts_received: usize = 0;

    // Connect ALL clients first. TCP handshakes complete at the kernel TCP layer
    // independently of io_uring and sit in the accept backlog.
    for (0..num_clients) |i| {
        client_fds[i] = try createClientSocket(port);
    }
    defer for (client_fds) |cfd| testClose(cfd);

    // Now harvest all accept CQEs from the single multishot SQE
    for (0..num_clients) |i| {
        const cqe = try ring.waitCqe();

        // The kernel may return EINVAL if multishot accept is not supported
        if (cqe.res < 0) {
            const errno = cqe.err();
            if (errno == .INVAL) {
                for (accepted_fds[0..accepts_received]) |afd| testClose(afd);
                return error.SkipZigTest;
            }
        }

        try std.testing.expect(cqe.res >= 0);
        try std.testing.expect(cqe.user_data == 42);
        accepted_fds[i] = @intCast(cqe.res);
        accepts_received = i + 1;

        // IORING_CQE_F_MORE should be set — the multishot accept SQE is still active
        try std.testing.expect(cqe.flags & linux.IORING_CQE_F_MORE > 0);
    }

    try std.testing.expectEqual(@as(usize, num_clients), accepts_received);

    for (accepted_fds[0..accepts_received]) |afd| testClose(afd);
}

test "Ring buffer group lifecycle with buffer recycling" {
    var ring = try Ring.init(16);
    defer ring.deinit();

    const allocator = std.testing.allocator;

    // Initialise a buffer group with 2 buffers of 128 bytes each
    const group_id: u16 = 1;
    const buffer_size: u32 = 128;
    const buffers_count: u16 = 2;
    var buf_grp = ring.initBufferGroup(allocator, group_id, buffer_size, buffers_count) catch |err| switch (err) {
        error.ArgumentsInvalid => return error.SkipZigTest,
        else => return err,
    };
    defer buf_grp.deinit(allocator);

    // Create a connected socket pair via listen + connect + accept
    const listen_fd = try createListenSocket();
    defer testClose(listen_fd);
    const port = try getBoundPort(listen_fd);

    const client_fd = try createClientSocket(port);
    defer testClose(client_fd);

    _ = try ring.prepAccept(listen_fd, 0);
    var cqe = try ring.waitCqe();
    try std.testing.expect(cqe.res >= 0);
    const server_fd: posix.fd_t = @intCast(cqe.res);
    defer testClose(server_fd);

    // Perform more rounds than the buffer count to prove recycling works
    const rounds = 6;
    for (0..rounds) |round| {
        _ = round;
        const msg = "hello buffer ring!";

        // Client sends data
        _ = try testWrite(client_fd, msg);

        // Server recvs using buffer group (kernel picks a buffer)
        _ = try buf_grp.recv(100, server_fd, 0);
        _ = try ring.submit();
        cqe = try ring.waitCqe();

        if (cqe.res < 0) {
            const errno = cqe.err();
            if (errno == .INVAL) return error.SkipZigTest;
            // ENOBUFS means we ran out of buffers - should not happen with proper recycling
            try std.testing.expect(errno != .NOBUFS);
        }

        try std.testing.expect(cqe.res > 0);
        try std.testing.expect(cqe.flags & linux.IORING_CQE_F_BUFFER == linux.IORING_CQE_F_BUFFER);

        // Get the buffer and verify data
        const buf = try buf_grp.get(cqe);
        try std.testing.expectEqualStrings(msg, buf);

        // Return the buffer to the kernel so it can be reused
        try buf_grp.put(cqe);
    }
}

test "Ring recv on closed fd returns negative result" {
    var ring = try Ring.init(16);
    defer ring.deinit();

    // Create a socket and immediately close it
    const fd = try testSocket();
    testClose(fd);

    // Try to recv on the closed fd via io_uring
    var buf: [64]u8 = undefined;
    _ = try ring.prepRecv(fd, &buf, 99);
    const cqe = try ring.waitCqe();

    // The kernel should report an error (negative res), not crash
    try std.testing.expect(cqe.res < 0);
}

test "Ring prepTimeout completes with ETIME" {
    var ring = try Ring.init(16);
    defer ring.deinit();

    var ts = linux.kernel_timespec{ .sec = 0, .nsec = 10_000_000 }; // 10ms
    _ = try ring.prepTimeout(&ts, 99);
    const cqe = try ring.waitCqe();

    // Timeout expiry returns -ETIME
    try std.testing.expect(cqe.res == -@as(i32, @intFromEnum(linux.E.TIME)));
    try std.testing.expect(cqe.user_data == 99);
}
