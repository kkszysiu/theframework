const std = @import("std");
const linux = std.os.linux;
const posix = std.posix;

// ---------------------------------------------------------------------------
// OpSlot
// ---------------------------------------------------------------------------

/// Coordination struct for green_poll_multi: multiple op slots share
/// a single PollGroup so that only the first CQE resumes the greenlet.
pub const PollGroup = struct {
    resumed: bool,
    greenlet: ?*anyopaque, // the greenlet to resume (incref'd once)
    active_waits_n: u32, // how many we added to hub.active_waits
};

/// Persistent storage for io_uring SQE parameters.
///
/// io_uring SQEs hold *pointers* to caller-owned memory (timespec,
/// sockaddr, etc.).  The SQEs are not submitted to the kernel until
/// submitAndWait() in the hub loop — by which time the greenlet that
/// prepared the SQE has switched out and its C stack has been reused.
/// Storing these values in the heap-allocated OpSlot keeps them alive.
pub const SqeStorage = union {
    timespec: linux.kernel_timespec,
    sockaddr: posix.sockaddr,
    poll_group: PollGroup,
};

pub const OpSlot = struct {
    greenlet: ?*anyopaque, // opaque — caller (hub.zig) handles incref/decref
    result: i32,
    generation: u32,
    slot_index: u16,
    poll_group: ?*PollGroup, // non-null only for green_poll_multi slots
    storage: SqeStorage, // persistent storage for SQE parameters

    pub fn reset(self: *OpSlot) void {
        // Note: caller must decref greenlet before calling reset
        self.greenlet = null;
        self.result = 0;
        self.poll_group = null;
    }
};

// ---------------------------------------------------------------------------
// user_data encoding (bit 63 discriminator)
//
// Layout: [bit 63: 1][bits 62-48: slot_index (15 bits)][bits 47-16: generation (32 bits)][bits 15-0: reserved]
//
// Bit 63 = 1 distinguishes OpSlot user_data from connection pool user_data
// (which always has bit 63 = 0).
// ---------------------------------------------------------------------------

const BIT_63: u64 = @as(u64, 1) << 63;

pub fn encodeUserData(slot_index: u16, generation: u32) u64 {
    return BIT_63 |
        (@as(u64, slot_index) << 48) |
        (@as(u64, generation) << 16);
}

pub fn decodeSlotIndex(user_data: u64) u16 {
    // Mask off bit 63 before shifting: bits 62-48
    return @intCast((user_data >> 48) & 0x7FFF);
}

pub fn decodeGeneration(user_data: u64) u32 {
    return @intCast((user_data >> 16) & 0xFFFFFFFF);
}

pub fn isOpSlot(user_data: u64) bool {
    return (user_data & BIT_63) != 0;
}

// ---------------------------------------------------------------------------
// OpSlotTable
// ---------------------------------------------------------------------------

pub const OP_SLOT_SIZE: u16 = 256;

pub const OpSlotTable = struct {
    slots: [OP_SLOT_SIZE]OpSlot,
    free_list: std.ArrayListUnmanaged(u16),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) !OpSlotTable {
        var table = OpSlotTable{
            .slots = undefined,
            .free_list = .empty,
            .allocator = allocator,
        };

        // Initialize all slots
        for (0..OP_SLOT_SIZE) |i| {
            const idx: u16 = @intCast(i);
            table.slots[i] = OpSlot{
                .greenlet = null,
                .result = 0,
                .generation = 0,
                .slot_index = idx,
                .poll_group = null,
                .storage = .{ .timespec = .{ .sec = 0, .nsec = 0 } },
            };
        }

        // Pre-populate free list (all slots are free)
        try table.free_list.ensureTotalCapacity(allocator, OP_SLOT_SIZE);
        var i: u16 = OP_SLOT_SIZE;
        while (i > 0) {
            i -= 1;
            table.free_list.appendAssumeCapacity(i);
        }

        return table;
    }

    pub fn deinit(self: *OpSlotTable) void {
        // Note: caller must decref any remaining greenlets before deinit
        self.free_list.deinit(self.allocator);
    }

    /// Acquire a free operation slot. Returns null if all slots are in use.
    pub fn acquire(self: *OpSlotTable) ?*OpSlot {
        const idx = self.free_list.pop() orelse return null;
        const slot = &self.slots[idx];
        slot.greenlet = null;
        slot.result = 0;
        slot.poll_group = null;
        return slot;
    }

    /// Release an operation slot back to the table. Increments generation to
    /// invalidate any stale CQEs. Caller must decref greenlet first.
    pub fn release(self: *OpSlotTable, slot: *OpSlot) void {
        slot.generation +%= 1; // wrapping add
        slot.reset();
        self.free_list.append(self.allocator, slot.slot_index) catch {
            // If this fails, we leak a slot. Acceptable for now.
        };
    }

    /// Look up an operation slot by user_data, validate generation.
    /// Returns null if slot_index is out of range or generation doesn't match (stale CQE).
    pub fn lookup(self: *OpSlotTable, user_data: u64) ?*OpSlot {
        const slot_index = decodeSlotIndex(user_data);
        const generation = decodeGeneration(user_data);

        if (slot_index >= OP_SLOT_SIZE) return null;

        const slot = &self.slots[slot_index];
        if (slot.generation != generation) return null;

        return slot;
    }

    /// How many operation slots are currently in use.
    pub fn activeCount(self: *OpSlotTable) usize {
        return OP_SLOT_SIZE - self.free_list.items.len;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "OpSlot user_data encode/decode round-trip" {
    const slot_index: u16 = 42;
    const generation: u32 = 12345;
    const encoded = encodeUserData(slot_index, generation);
    try std.testing.expectEqual(slot_index, decodeSlotIndex(encoded));
    try std.testing.expectEqual(generation, decodeGeneration(encoded));
    try std.testing.expect(isOpSlot(encoded));
}

test "OpSlot user_data has bit 63 set" {
    const encoded = encodeUserData(7, 999);
    // Bit 63 must be set for OpSlot user_data
    try std.testing.expect((encoded & BIT_63) != 0);

    // Connection pool user_data must NOT have bit 63 set.
    // Connection pool encoding: [16 bits: pool_index][32 bits: generation][16 bits: reserved]
    // Even with max values, bit 63 is a pool_index bit — but pool_index max is
    // 4095, which is well below the 15-bit threshold needed to reach bit 63.
    const conn_user_data: u64 = (@as(u64, 4095) << 48) | (@as(u64, 0xFFFFFFFF) << 16);
    try std.testing.expect((conn_user_data & BIT_63) == 0);
}

test "OpSlotTable acquire and release" {
    const allocator = std.testing.allocator;
    var table = try OpSlotTable.init(allocator);
    defer table.deinit();

    // All slots should be free initially
    try std.testing.expectEqual(@as(usize, 0), table.activeCount());

    // Acquire a slot
    const slot1 = table.acquire() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), table.activeCount());

    const idx1 = slot1.slot_index;
    const gen1 = slot1.generation;

    // Release it
    table.release(slot1);
    try std.testing.expectEqual(@as(usize, 0), table.activeCount());

    // Acquire again — should get the same slot but incremented generation
    const slot2 = table.acquire() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(idx1, slot2.slot_index);
    try std.testing.expectEqual(gen1 +% 1, slot2.generation);

    table.release(slot2);
}

test "OpSlotTable acquire N release all acquire N" {
    const allocator = std.testing.allocator;
    var table = try OpSlotTable.init(allocator);
    defer table.deinit();

    const N = 10;
    var slots: [N]*OpSlot = undefined;

    // Acquire N
    for (0..N) |i| {
        slots[i] = table.acquire() orelse return error.TestUnexpectedResult;
    }
    try std.testing.expectEqual(@as(usize, N), table.activeCount());

    // Store generations
    var gens: [N]u32 = undefined;
    for (0..N) |i| {
        gens[i] = slots[i].generation;
    }

    // Release all
    for (0..N) |i| {
        table.release(slots[i]);
    }
    try std.testing.expectEqual(@as(usize, 0), table.activeCount());

    // Acquire N again — generations should be incremented
    for (0..N) |i| {
        slots[i] = table.acquire() orelse return error.TestUnexpectedResult;
    }
    try std.testing.expectEqual(@as(usize, N), table.activeCount());

    // All acquired slots should have incremented generations
    for (0..N) |i| {
        const idx = slots[i].slot_index;
        try std.testing.expectEqual(gens[idx] +% 1, slots[i].generation);
    }

    // Release all
    for (0..N) |i| {
        table.release(slots[i]);
    }
}

test "OpSlotTable lookup validates generation" {
    const allocator = std.testing.allocator;
    var table = try OpSlotTable.init(allocator);
    defer table.deinit();

    // Acquire a slot
    const slot = table.acquire() orelse return error.TestUnexpectedResult;
    const user_data = encodeUserData(slot.slot_index, slot.generation);

    // Lookup should succeed
    const found = table.lookup(user_data);
    try std.testing.expect(found != null);
    try std.testing.expectEqual(slot.slot_index, found.?.slot_index);

    // Release and re-acquire (bumps generation)
    table.release(slot);
    _ = table.acquire() orelse return error.TestUnexpectedResult;

    // Lookup with OLD user_data should fail (stale generation)
    const stale = table.lookup(user_data);
    try std.testing.expect(stale == null);
}

test "OpSlotTable exhaustion" {
    const allocator = std.testing.allocator;
    var table = try OpSlotTable.init(allocator);
    defer table.deinit();

    // Exhaust all slots
    for (0..OP_SLOT_SIZE) |_| {
        _ = table.acquire() orelse return error.TestUnexpectedResult;
    }

    // Next acquire should return null
    try std.testing.expect(table.acquire() == null);
}
