const std = @import("std");
const posix = std.posix;
const arena_mod = @import("arena.zig");
const RequestArena = arena_mod.RequestArena;
const chunk_pool = @import("chunk_pool.zig");
const Chunk = chunk_pool.Chunk;
const ChunkRecycler = chunk_pool.ChunkRecycler;
const config_mod = @import("config.zig");
const ArenaConfig = config_mod.ArenaConfig;
const input_buffer_mod = @import("input_buffer.zig");
const InputBuffer = input_buffer_mod.InputBuffer;
const BufferRecycler = @import("buffer_recycler.zig").BufferRecycler;

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

pub const ConnectionState = enum(u8) {
    idle,
    reading,
    writing,
    cancelling,
    closing,
};

pub const Connection = struct {
    fd: posix.fd_t,
    state: ConnectionState,
    generation: u32,
    greenlet: ?*anyopaque, // opaque -- caller (hub.zig) handles incref/decref
    pending_ops: u8,
    pool_index: u16,
    result: i32, // CQE result storage

    // Heap-resident iovec array for writev response sending.
    // Must NOT be on the greenlet stack: when a greenlet yields, the greenlet
    // library copies its stack frame to a heap buffer and reuses the original
    // stack address for other greenlets.  io_uring SQEs store a pointer to
    // the iovec array, so it must remain at a stable address until the kernel
    // processes the SQE.
    send_iovecs: [2]posix.iovec_const,

    // Per-connection arena allocator (replaces recv_buf/send_buf)
    arena: RequestArena,
    inline_chunk: Chunk, // 4 KB embedded, used as arena's first_chunk

    // Per-connection input buffer for zero-copy recv accumulation
    input: InputBuffer,
    inline_buf: [input_buffer_mod.INLINE_BUF_SIZE]u8, // 4 KB embedded

    pub fn reset(self: *Connection) void {
        // Note: caller must decref greenlet before calling reset
        self.fd = -1;
        self.state = .idle;
        self.greenlet = null;
        self.pending_ops = 0;
        self.result = 0;
        self.arena.reset();
        self.input.resetForNewConnection(&self.inline_buf, input_buffer_mod.INLINE_BUF_SIZE);
    }
};

// ---------------------------------------------------------------------------
// user_data encoding: [16 bits: pool_index][32 bits: generation][16 bits: reserved]
// ---------------------------------------------------------------------------

pub fn encodeUserData(pool_index: u16, generation: u32) u64 {
    return (@as(u64, pool_index) << 48) | (@as(u64, generation) << 16);
}

pub fn decodePoolIndex(user_data: u64) u16 {
    return @intCast(user_data >> 48);
}

pub fn decodeGeneration(user_data: u64) u32 {
    return @intCast((user_data >> 16) & 0xFFFFFFFF);
}

// ---------------------------------------------------------------------------
// ConnectionPool
// ---------------------------------------------------------------------------

pub const ConnectionPool = struct {
    connections: []Connection, // heap-allocated, dynamically sized
    free_list: std.ArrayListUnmanaged(u16),
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        config: ArenaConfig,
        recycler: *ChunkRecycler,
        buf_recycler: *BufferRecycler,
    ) !ConnectionPool {
        const max_conns: usize = config.max_connections;
        const connections = try allocator.alloc(Connection, max_conns);
        errdefer allocator.free(connections);

        // Initialize all connections
        for (0..max_conns) |i| {
            const idx: u16 = @intCast(i);
            connections[i] = Connection{
                .fd = -1,
                .state = .idle,
                .generation = 0,
                .greenlet = null,
                .pending_ops = 0,
                .pool_index = idx,
                .result = 0,
                .send_iovecs = undefined,
                .inline_chunk = undefined,
                .arena = undefined,
                .inline_buf = undefined,
                .input = undefined,
            };
            connections[i].arena = RequestArena.init(
                &connections[i].inline_chunk,
                recycler,
                config.directThreshold(),
            );
            connections[i].input = InputBuffer.init(
                &connections[i].inline_buf,
                input_buffer_mod.INLINE_BUF_SIZE,
                buf_recycler,
            );
        }

        // Pre-populate free list (all slots are free)
        var free_list: std.ArrayListUnmanaged(u16) = .empty;
        try free_list.ensureTotalCapacity(allocator, max_conns);
        var i: u16 = @intCast(max_conns);
        while (i > 0) {
            i -= 1;
            free_list.appendAssumeCapacity(i);
        }

        return ConnectionPool{
            .connections = connections,
            .free_list = free_list,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ConnectionPool) void {
        // Note: caller must decref any remaining greenlets before deinit
        // Reset all arenas to return chunks to recycler and free directs
        // Reset all input buffers to return buffers to recycler
        for (self.connections) |*conn| {
            conn.arena.reset();
            conn.input.resetForNewConnection(&conn.inline_buf, input_buffer_mod.INLINE_BUF_SIZE);
        }
        self.allocator.free(self.connections);
        self.free_list.deinit(self.allocator);
    }

    /// Acquire a free connection slot. Returns null if pool is exhausted.
    pub fn acquire(self: *ConnectionPool) ?*Connection {
        const idx = self.free_list.pop() orelse return null;
        const conn = &self.connections[idx];
        conn.state = .idle;
        conn.fd = -1;
        conn.pending_ops = 0;
        conn.result = 0;
        return conn;
    }

    /// Release a connection back to the pool. Increments generation to
    /// invalidate any stale CQEs. Caller must decref greenlet first.
    pub fn release(self: *ConnectionPool, conn: *Connection) void {
        conn.generation +%= 1; // wrapping add
        conn.reset(); // calls arena.reset()
        self.free_list.append(self.allocator, conn.pool_index) catch {
            // If this fails, we leak a slot. Acceptable for now.
        };
    }

    /// Look up a connection by pool_index, validate generation.
    /// Returns null if the generation doesn't match (stale CQE).
    pub fn lookup(self: *ConnectionPool, user_data: u64) ?*Connection {
        const pool_index = decodePoolIndex(user_data);
        const generation = decodeGeneration(user_data);

        if (pool_index >= self.connections.len) return null;

        const conn = &self.connections[pool_index];
        if (conn.generation != generation) return null;

        return conn;
    }

    /// Fix up all arena recycler pointers to point to the given recycler.
    /// Must be called after the Hub (which owns the recycler) is placed at
    /// its final address, because Hub.init() returns by value and the
    /// recycler moves to a new address on assignment.
    pub fn fixupRecycler(self: *ConnectionPool, recycler: *ChunkRecycler) void {
        for (self.connections) |*conn| {
            conn.arena.recycler = recycler;
        }
    }

    /// Fix up all InputBuffer recycler pointers to point to the given BufferRecycler.
    /// Must be called after the Hub is placed at its final address (same reason
    /// as fixupRecycler for the ChunkRecycler).
    pub fn fixupBufRecycler(self: *ConnectionPool, buf_recycler: *BufferRecycler) void {
        for (self.connections) |*conn| {
            conn.input.recycler = buf_recycler;
        }
    }

    /// How many connections are currently in use.
    pub fn activeCount(self: *ConnectionPool) usize {
        return self.connections.len - self.free_list.items.len;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "user_data encode/decode round-trip" {
    const pool_index: u16 = 42;
    const generation: u32 = 12345;
    const encoded = encodeUserData(pool_index, generation);
    try std.testing.expectEqual(pool_index, decodePoolIndex(encoded));
    try std.testing.expectEqual(generation, decodeGeneration(encoded));
}

test "user_data encode/decode max values" {
    const pool_index: u16 = std.math.maxInt(u16);
    const generation: u32 = std.math.maxInt(u32);
    const encoded = encodeUserData(pool_index, generation);
    try std.testing.expectEqual(pool_index, decodePoolIndex(encoded));
    try std.testing.expectEqual(generation, decodeGeneration(encoded));
}

test "ConnectionPool acquire and release" {
    const allocator = std.testing.allocator;

    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    var buf_recycler = BufferRecycler.init(allocator);
    defer buf_recycler.deinit();

    var config = ArenaConfig.defaults();
    config.max_connections = 64; // small pool for testing
    var pool = try ConnectionPool.init(allocator, config, &recycler, &buf_recycler);
    defer pool.deinit();

    // All slots should be free initially
    try std.testing.expectEqual(@as(usize, 0), pool.activeCount());

    // Acquire a connection
    const conn1 = pool.acquire() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), pool.activeCount());
    try std.testing.expectEqual(ConnectionState.idle, conn1.state);

    const idx1 = conn1.pool_index;
    const gen1 = conn1.generation;

    // Release it
    pool.release(conn1);
    try std.testing.expectEqual(@as(usize, 0), pool.activeCount());

    // Acquire again -- should get the same slot but incremented generation
    const conn2 = pool.acquire() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(idx1, conn2.pool_index);
    try std.testing.expectEqual(gen1 +% 1, conn2.generation);

    pool.release(conn2);
}

test "ConnectionPool acquire N, release all, acquire N again" {
    const allocator = std.testing.allocator;

    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    var buf_recycler = BufferRecycler.init(allocator);
    defer buf_recycler.deinit();

    var config = ArenaConfig.defaults();
    config.max_connections = 64;
    var pool = try ConnectionPool.init(allocator, config, &recycler, &buf_recycler);
    defer pool.deinit();

    const N = 50;
    var conns: [N]*Connection = undefined;

    // Acquire N
    for (0..N) |i| {
        conns[i] = pool.acquire() orelse return error.TestUnexpectedResult;
        conns[i].fd = @intCast(100 + i);
    }
    try std.testing.expectEqual(@as(usize, N), pool.activeCount());

    // Store generations
    var gens: [N]u32 = undefined;
    for (0..N) |i| {
        gens[i] = conns[i].generation;
    }

    // Release all
    for (0..N) |i| {
        pool.release(conns[i]);
    }
    try std.testing.expectEqual(@as(usize, 0), pool.activeCount());

    // Acquire N again -- generations should be incremented
    for (0..N) |i| {
        conns[i] = pool.acquire() orelse return error.TestUnexpectedResult;
    }
    try std.testing.expectEqual(@as(usize, N), pool.activeCount());

    // All acquired connections should have incremented generations
    for (0..N) |i| {
        const idx = conns[i].pool_index;
        try std.testing.expectEqual(gens[idx] +% 1, conns[i].generation);
    }

    // Release all
    for (0..N) |i| {
        pool.release(conns[i]);
    }
}

test "ConnectionPool lookup validates generation" {
    const allocator = std.testing.allocator;

    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    var buf_recycler = BufferRecycler.init(allocator);
    defer buf_recycler.deinit();

    var config = ArenaConfig.defaults();
    config.max_connections = 64;
    var pool = try ConnectionPool.init(allocator, config, &recycler, &buf_recycler);
    defer pool.deinit();

    // Acquire a connection
    const conn = pool.acquire() orelse return error.TestUnexpectedResult;
    const user_data = encodeUserData(conn.pool_index, conn.generation);

    // Lookup should succeed
    const found = pool.lookup(user_data);
    try std.testing.expect(found != null);
    try std.testing.expectEqual(conn.pool_index, found.?.pool_index);

    // Release and re-acquire (bumps generation)
    pool.release(conn);
    _ = pool.acquire() orelse return error.TestUnexpectedResult;

    // Lookup with OLD user_data should fail (stale generation)
    const stale = pool.lookup(user_data);
    try std.testing.expect(stale == null);
}

test "ConnectionPool exhaustion" {
    const allocator = std.testing.allocator;

    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    var buf_recycler = BufferRecycler.init(allocator);
    defer buf_recycler.deinit();

    var config = ArenaConfig.defaults();
    config.max_connections = 8; // tiny pool
    var pool = try ConnectionPool.init(allocator, config, &recycler, &buf_recycler);
    defer pool.deinit();

    // Exhaust all slots
    for (0..8) |_| {
        _ = pool.acquire() orelse return error.TestUnexpectedResult;
    }

    // Next acquire should return null
    try std.testing.expect(pool.acquire() == null);
}

test "Connection arena integration: alloc, reset, reuse" {
    const allocator = std.testing.allocator;

    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    var buf_recycler = BufferRecycler.init(allocator);
    defer buf_recycler.deinit();

    var config = ArenaConfig.defaults();
    config.max_connections = 4;
    var pool = try ConnectionPool.init(allocator, config, &recycler, &buf_recycler);
    defer pool.deinit();

    // Acquire a connection
    const conn = pool.acquire() orelse return error.TestUnexpectedResult;

    // Use the arena (simulating currentFreeSlice + commitBytes for recv)
    const slice = conn.arena.currentFreeSlice(256) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 256), slice.len);
    @memset(slice[0..13], 'H');
    conn.arena.commitBytes(13);

    // Release (calls reset via pool.release -> conn.reset -> arena.reset)
    pool.release(conn);

    // Acquire again and verify arena is fresh
    const conn2 = pool.acquire() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(Chunk.DATA_START, conn2.arena.chunk_offset);

    pool.release(conn2);
}
