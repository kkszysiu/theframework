const std = @import("std");

/// A 4 KB chunk. The `next` pointer overlays the first 8 bytes of the
/// payload (union trick for zero-overhead linked list).
pub const Chunk = extern union {
    next: ?*Chunk,
    bytes: [CHUNK_SIZE]u8,

    pub const CHUNK_SIZE: usize = 4096;
    /// Offset where bump allocation starts (after the next pointer).
    pub const DATA_START: usize = @sizeOf(?*Chunk);
    /// Usable bytes per chunk.
    pub const USABLE: usize = CHUNK_SIZE - DATA_START;
};

/// Hub-local LIFO stack of free chunks. Chunks return here on
/// arena.reset() and are grabbed from here on arena overflow.
/// In steady state, no malloc/free happens.
pub const ChunkRecycler = struct {
    free_stack: std.ArrayListUnmanaged(*Chunk),
    allocator: std.mem.Allocator,
    total_allocated: usize, // chunks ever malloc'd (for monitoring)

    pub fn init(allocator: std.mem.Allocator) ChunkRecycler {
        return .{ .free_stack = .empty, .allocator = allocator, .total_allocated = 0 };
    }

    pub fn deinit(self: *ChunkRecycler) void {
        for (self.free_stack.items) |chunk| {
            self.allocator.destroy(chunk);
        }
        self.free_stack.deinit(self.allocator);
    }

    /// Pop from stack (LIFO, cache-warm) or malloc a new chunk.
    pub fn acquire(self: *ChunkRecycler) !*Chunk {
        if (self.free_stack.pop()) |chunk| return chunk;
        const chunk = try self.allocator.create(Chunk);
        self.total_allocated += 1;
        return chunk;
    }

    /// Push onto stack. No free() -- cached for reuse.
    pub fn release(self: *ChunkRecycler, chunk: *Chunk) void {
        self.free_stack.append(self.allocator, chunk) catch {
            self.allocator.destroy(chunk); // OOM growing stack -> free chunk
        };
    }

    pub fn cachedCount(self: *const ChunkRecycler) usize {
        return self.free_stack.items.len;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "Chunk size is 4096" {
    try std.testing.expectEqual(@as(usize, 4096), @sizeOf(Chunk));
}

test "Chunk.CHUNK_SIZE equals @sizeOf(Chunk)" {
    try std.testing.expectEqual(Chunk.CHUNK_SIZE, @sizeOf(Chunk));
}

test "Chunk.DATA_START equals @sizeOf(?*Chunk)" {
    try std.testing.expectEqual(@sizeOf(?*Chunk), Chunk.DATA_START);
}

test "Chunk.DATA_START is 8 on 64-bit" {
    try std.testing.expectEqual(@as(usize, 8), Chunk.DATA_START);
}

test "Chunk.USABLE is 4088" {
    try std.testing.expectEqual(@as(usize, 4088), Chunk.USABLE);
}

test "acquire from empty recycler returns newly allocated chunk" {
    const allocator = std.testing.allocator;
    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    try std.testing.expectEqual(@as(usize, 0), recycler.cachedCount());
    try std.testing.expectEqual(@as(usize, 0), recycler.total_allocated);

    const chunk = try recycler.acquire();
    // Should have allocated a new chunk
    try std.testing.expectEqual(@as(usize, 1), recycler.total_allocated);
    // Stack should still be empty (chunk is in use, not cached)
    try std.testing.expectEqual(@as(usize, 0), recycler.cachedCount());

    // Release so deinit can free it
    recycler.release(chunk);
}

test "acquire then release then acquire returns same pointer (LIFO reuse)" {
    const allocator = std.testing.allocator;
    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    // Acquire a new chunk
    const chunk1 = try recycler.acquire();
    const ptr1 = @intFromPtr(chunk1);

    // Release it back
    recycler.release(chunk1);
    try std.testing.expectEqual(@as(usize, 1), recycler.cachedCount());

    // Acquire again -- should get the same pointer (LIFO)
    const chunk2 = try recycler.acquire();
    const ptr2 = @intFromPtr(chunk2);
    try std.testing.expectEqual(ptr1, ptr2);

    // total_allocated should still be 1 (reused, not newly allocated)
    try std.testing.expectEqual(@as(usize, 1), recycler.total_allocated);

    // Release so deinit can free it
    recycler.release(chunk2);
}

test "cachedCount increments on release, decrements on acquire" {
    const allocator = std.testing.allocator;
    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    // Start empty
    try std.testing.expectEqual(@as(usize, 0), recycler.cachedCount());

    // Allocate 3 chunks
    const c1 = try recycler.acquire();
    const c2 = try recycler.acquire();
    const c3 = try recycler.acquire();
    try std.testing.expectEqual(@as(usize, 0), recycler.cachedCount());

    // Release them one by one
    recycler.release(c1);
    try std.testing.expectEqual(@as(usize, 1), recycler.cachedCount());

    recycler.release(c2);
    try std.testing.expectEqual(@as(usize, 2), recycler.cachedCount());

    recycler.release(c3);
    try std.testing.expectEqual(@as(usize, 3), recycler.cachedCount());

    // Acquire one -- cachedCount should decrease
    const c4 = try recycler.acquire();
    try std.testing.expectEqual(@as(usize, 2), recycler.cachedCount());

    // total_allocated unchanged (reused from cache)
    try std.testing.expectEqual(@as(usize, 3), recycler.total_allocated);

    // Release c4 so deinit can free everything
    recycler.release(c4);
}

test "deinit frees all cached chunks (no leak under test allocator)" {
    const allocator = std.testing.allocator;
    var recycler = ChunkRecycler.init(allocator);

    // Acquire and release several chunks
    const c1 = try recycler.acquire();
    const c2 = try recycler.acquire();
    const c3 = try recycler.acquire();

    recycler.release(c1);
    recycler.release(c2);
    recycler.release(c3);

    try std.testing.expectEqual(@as(usize, 3), recycler.cachedCount());

    // deinit should free all 3 chunks and the stack itself.
    // If any leak, the test allocator will report it as a test failure.
    recycler.deinit();
}

test "LIFO ordering: last released is first acquired" {
    const allocator = std.testing.allocator;
    var recycler = ChunkRecycler.init(allocator);
    defer recycler.deinit();

    const c1 = try recycler.acquire();
    const c2 = try recycler.acquire();

    const ptr1 = @intFromPtr(c1);
    const ptr2 = @intFromPtr(c2);

    // Release c1 first, then c2
    recycler.release(c1);
    recycler.release(c2);

    // Acquire: should get c2 first (LIFO)
    const got1 = try recycler.acquire();
    try std.testing.expectEqual(ptr2, @intFromPtr(got1));

    // Then c1
    const got2 = try recycler.acquire();
    try std.testing.expectEqual(ptr1, @intFromPtr(got2));

    // Release both for cleanup
    recycler.release(got1);
    recycler.release(got2);
}
