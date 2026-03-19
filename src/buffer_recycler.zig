const std = @import("std");

/// Minimum power-of-2 exponent for buffer size classes: 2^12 = 4096.
pub const MIN_POWER: u5 = 12;

/// Maximum power-of-2 exponent for buffer size classes: 2^24 = 16 MB.
pub const MAX_POWER: u5 = 24;

/// Number of bins: one per power-of-2 size class from MIN_POWER to MAX_POWER inclusive.
pub const NUM_BINS: usize = MAX_POWER - MIN_POWER + 1; // 13

/// Power-of-2 buffer recycler, modeled on h2o's buffer bin system.
/// One per hub (hub-local, effectively thread-local per worker process).
///
/// Buffers are recycled by size class. Each bin is a LIFO stack of buffers
/// of that power-of-2 size. On `acquire`, we pop from the appropriate bin
/// (cache-warm reuse) or fall back to malloc. On `release`, we push to the
/// appropriate bin.
pub const BufferRecycler = struct {
    /// One LIFO stack per power-of-2 size class.
    /// bins[0] = 4 KB, bins[1] = 8 KB, ..., bins[12] = 16 MB.
    bins: [NUM_BINS]std.ArrayListUnmanaged([*]u8),

    /// Backing allocator for malloc/free of buffers and bin arrays.
    allocator: std.mem.Allocator,

    /// Total number of buffers ever malloc'd (for monitoring).
    total_allocated: usize,

    /// Initialize with empty bins.
    pub fn init(allocator: std.mem.Allocator) BufferRecycler {
        var bins: [NUM_BINS]std.ArrayListUnmanaged([*]u8) = undefined;
        for (&bins) |*bin| {
            bin.* = .empty;
        }
        return .{
            .bins = bins,
            .allocator = allocator,
            .total_allocated = 0,
        };
    }

    /// Free all cached buffers and bin arrays.
    pub fn deinit(self: *BufferRecycler) void {
        for (&self.bins, 0..) |*bin, i| {
            const power: u5 = @intCast(i + MIN_POWER);
            const buf_size: usize = @as(usize, 1) << power;
            for (bin.items) |buf_ptr| {
                self.allocator.free(buf_ptr[0..buf_size]);
            }
            bin.deinit(self.allocator);
        }
    }

    /// Acquire a buffer of at least `min_size` bytes.
    /// Returns a slice of the next power-of-2 size >= min_size.
    /// Tries recycled first (LIFO, cache-warm), falls back to malloc.
    pub fn acquire(self: *BufferRecycler, min_size: usize) ![]u8 {
        const power = sizeToPower(min_size);
        const bin_idx: usize = power - MIN_POWER;
        const actual_size: usize = @as(usize, 1) << power;

        // Try recycled first (LIFO, cache-warm)
        if (self.bins[bin_idx].pop()) |buf_ptr| {
            return buf_ptr[0..actual_size];
        }

        // Cold path: malloc
        const buf = try self.allocator.alloc(u8, actual_size);
        self.total_allocated += 1;
        return buf;
    }

    /// Return a buffer to the appropriate size-class bin.
    /// The buffer length must be a power of 2 in the supported range.
    pub fn release(self: *BufferRecycler, buf: []u8) void {
        const power = sizeToPower(buf.len);
        const bin_idx: usize = power - MIN_POWER;
        self.bins[bin_idx].append(self.allocator, buf.ptr) catch {
            // OOM growing bin array -- free the buffer rather than leak it
            self.allocator.free(buf);
        };
    }

    /// Return the number of cached buffers across all bins.
    pub fn cachedCount(self: *const BufferRecycler) usize {
        var count: usize = 0;
        for (&self.bins) |*bin| {
            count += bin.items.len;
        }
        return count;
    }
};

/// Round up to the next power-of-2, returning the exponent.
/// Clamped to [MIN_POWER, MAX_POWER].
///
/// Examples:
///   sizeToPower(1) → 12  (clamped to min 4096)
///   sizeToPower(4095) → 12  (rounds up to 4096)
///   sizeToPower(4096) → 12  (exact fit)
///   sizeToPower(4097) → 13  (rounds up to 8192)
///   sizeToPower(16 MB + 1) → 24  (clamped to max 16 MB)
pub fn sizeToPower(size: usize) u5 {
    if (size <= (@as(usize, 1) << MIN_POWER)) return MIN_POWER;
    // @clz gives the number of leading zeros. @bitSizeOf(usize) - @clz(size - 1)
    // gives the number of bits needed, which equals the power-of-2 exponent for
    // the next power of 2 >= size.
    const bits: u5 = @intCast(@bitSizeOf(usize) - @clz(size - 1));
    return @min(bits, MAX_POWER);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "sizeToPower: values at or below MIN_POWER return MIN_POWER" {
    try std.testing.expectEqual(@as(u5, 12), sizeToPower(0));
    try std.testing.expectEqual(@as(u5, 12), sizeToPower(1));
    try std.testing.expectEqual(@as(u5, 12), sizeToPower(4095));
    try std.testing.expectEqual(@as(u5, 12), sizeToPower(4096));
}

test "sizeToPower: rounds up to next power of 2" {
    try std.testing.expectEqual(@as(u5, 13), sizeToPower(4097));
    try std.testing.expectEqual(@as(u5, 13), sizeToPower(8192));
    try std.testing.expectEqual(@as(u5, 14), sizeToPower(8193));
    try std.testing.expectEqual(@as(u5, 14), sizeToPower(16384));
    try std.testing.expectEqual(@as(u5, 15), sizeToPower(16385));
}

test "sizeToPower: exact powers of 2" {
    try std.testing.expectEqual(@as(u5, 12), sizeToPower(4096));
    try std.testing.expectEqual(@as(u5, 13), sizeToPower(8192));
    try std.testing.expectEqual(@as(u5, 16), sizeToPower(65536));
    try std.testing.expectEqual(@as(u5, 20), sizeToPower(1 << 20));
    try std.testing.expectEqual(@as(u5, 24), sizeToPower(1 << 24));
}

test "sizeToPower: clamped to MAX_POWER" {
    try std.testing.expectEqual(@as(u5, 24), sizeToPower((1 << 24) + 1));
    try std.testing.expectEqual(@as(u5, 24), sizeToPower(1 << 25));
}

test "acquire from empty bin returns newly allocated buffer" {
    const allocator = std.testing.allocator;
    var recycler = BufferRecycler.init(allocator);
    defer recycler.deinit();

    try std.testing.expectEqual(@as(usize, 0), recycler.total_allocated);
    try std.testing.expectEqual(@as(usize, 0), recycler.cachedCount());

    const buf = try recycler.acquire(4096);
    try std.testing.expectEqual(@as(usize, 4096), buf.len);
    try std.testing.expectEqual(@as(usize, 1), recycler.total_allocated);

    // Release so deinit can free it
    recycler.release(buf);
}

test "release + acquire returns same pointer (LIFO reuse)" {
    const allocator = std.testing.allocator;
    var recycler = BufferRecycler.init(allocator);
    defer recycler.deinit();

    // Acquire a buffer
    const buf1 = try recycler.acquire(4096);
    const ptr1 = @intFromPtr(buf1.ptr);

    // Release it back
    recycler.release(buf1);
    try std.testing.expectEqual(@as(usize, 1), recycler.cachedCount());

    // Acquire again -- should get the same pointer (LIFO)
    const buf2 = try recycler.acquire(4096);
    const ptr2 = @intFromPtr(buf2.ptr);
    try std.testing.expectEqual(ptr1, ptr2);

    // total_allocated should still be 1 (reused, not newly allocated)
    try std.testing.expectEqual(@as(usize, 1), recycler.total_allocated);

    recycler.release(buf2);
}

test "bins are independent: different size classes don't interfere" {
    const allocator = std.testing.allocator;
    var recycler = BufferRecycler.init(allocator);
    defer recycler.deinit();

    // Acquire from two different size classes
    const buf_4k = try recycler.acquire(4096);
    const buf_8k = try recycler.acquire(8192);
    try std.testing.expectEqual(@as(usize, 4096), buf_4k.len);
    try std.testing.expectEqual(@as(usize, 8192), buf_8k.len);

    const ptr_4k = @intFromPtr(buf_4k.ptr);
    const ptr_8k = @intFromPtr(buf_8k.ptr);

    // Release both
    recycler.release(buf_4k);
    recycler.release(buf_8k);
    try std.testing.expectEqual(@as(usize, 2), recycler.cachedCount());

    // Acquire 4K again -- should get back the 4K pointer, not the 8K one
    const got_4k = try recycler.acquire(4096);
    try std.testing.expectEqual(ptr_4k, @intFromPtr(got_4k.ptr));
    try std.testing.expectEqual(@as(usize, 4096), got_4k.len);

    // Acquire 8K -- should get back the 8K pointer
    const got_8k = try recycler.acquire(8192);
    try std.testing.expectEqual(ptr_8k, @intFromPtr(got_8k.ptr));
    try std.testing.expectEqual(@as(usize, 8192), got_8k.len);

    recycler.release(got_4k);
    recycler.release(got_8k);
}

test "acquire with non-power-of-2 size rounds up" {
    const allocator = std.testing.allocator;
    var recycler = BufferRecycler.init(allocator);
    defer recycler.deinit();

    // Request 5000 bytes -- should get an 8192-byte buffer (2^13)
    const buf = try recycler.acquire(5000);
    try std.testing.expectEqual(@as(usize, 8192), buf.len);

    recycler.release(buf);
}

test "deinit frees all cached buffers (no leak under test allocator)" {
    const allocator = std.testing.allocator;
    var recycler = BufferRecycler.init(allocator);

    // Acquire and release several buffers of different sizes
    const b1 = try recycler.acquire(4096);
    const b2 = try recycler.acquire(8192);
    const b3 = try recycler.acquire(16384);
    const b4 = try recycler.acquire(4096);

    recycler.release(b1);
    recycler.release(b2);
    recycler.release(b3);
    recycler.release(b4);

    try std.testing.expectEqual(@as(usize, 4), recycler.cachedCount());

    // deinit should free all cached buffers and bin arrays.
    // If any leak, the test allocator will report it.
    recycler.deinit();
}

test "LIFO ordering: last released is first acquired within same bin" {
    const allocator = std.testing.allocator;
    var recycler = BufferRecycler.init(allocator);
    defer recycler.deinit();

    const b1 = try recycler.acquire(4096);
    const b2 = try recycler.acquire(4096);

    const ptr1 = @intFromPtr(b1.ptr);
    const ptr2 = @intFromPtr(b2.ptr);

    // Release b1 first, then b2
    recycler.release(b1);
    recycler.release(b2);

    // Acquire: should get b2 first (LIFO)
    const got1 = try recycler.acquire(4096);
    try std.testing.expectEqual(ptr2, @intFromPtr(got1.ptr));

    // Then b1
    const got2 = try recycler.acquire(4096);
    try std.testing.expectEqual(ptr1, @intFromPtr(got2.ptr));

    recycler.release(got1);
    recycler.release(got2);
}

test "acquire small size gets minimum 4 KB buffer" {
    const allocator = std.testing.allocator;
    var recycler = BufferRecycler.init(allocator);
    defer recycler.deinit();

    // Even a 1-byte request should get a 4096-byte buffer
    const buf = try recycler.acquire(1);
    try std.testing.expectEqual(@as(usize, 4096), buf.len);

    recycler.release(buf);
}
