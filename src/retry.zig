const std = @import("std");

pub const ExponentialBackoff = struct {
    base_seconds: i64 = 15,
    max_seconds: i64 = 3600,
    jitter: bool = true,
};

pub const LinearBackoff = struct {
    delay_seconds: i64 = 60,
};

pub const ConstantBackoff = struct {
    delay_seconds: i64 = 30,
};

pub const RetryStrategy = union(enum) {
    exponential: ExponentialBackoff,
    linear: LinearBackoff,
    constant: ConstantBackoff,
    custom: *const fn (attempt: i32, base_delay: i64) i64,
};

pub fn nextRetryAt(strategy: RetryStrategy, attempt: i32, now: i64) i64 {
    const delay: i64 = switch (strategy) {
        .exponential => |exp| blk: {
            const shift: u6 = @intCast(@min(attempt, 30));
            const raw: i64 = exp.base_seconds * (@as(i64, 1) << shift);
            var capped = @min(raw, exp.max_seconds);
            if (exp.jitter) {
                // Deterministic jitter based on attempt + now to avoid needing runtime RNG
                const jitter_range = @max(@divTrunc(capped, 4), 1);
                const jitter_val = @mod(@as(i64, @intCast(attempt)) *% 7 +% now, jitter_range);
                capped += jitter_val;
            }
            break :blk capped;
        },
        .linear => |lin| lin.delay_seconds * @as(i64, @intCast(attempt + 1)),
        .constant => |con| con.delay_seconds,
        .custom => |func| func(attempt, 0),
    };
    return now + delay;
}

// ── Tests ──────────────────────────────────────────────────────────────

test "exponential backoff delays" {
    const strategy = RetryStrategy{ .exponential = .{ .base_seconds = 10, .max_seconds = 3600, .jitter = false } };
    // attempt 0: 10 * 2^0 = 10
    try std.testing.expectEqual(@as(i64, 1010), nextRetryAt(strategy, 0, 1000));
    // attempt 1: 10 * 2^1 = 20
    try std.testing.expectEqual(@as(i64, 1020), nextRetryAt(strategy, 1, 1000));
    // attempt 2: 10 * 2^2 = 40
    try std.testing.expectEqual(@as(i64, 1040), nextRetryAt(strategy, 2, 1000));
    // attempt 3: 10 * 2^3 = 80
    try std.testing.expectEqual(@as(i64, 1080), nextRetryAt(strategy, 3, 1000));
    // attempt 4: 10 * 2^4 = 160
    try std.testing.expectEqual(@as(i64, 1160), nextRetryAt(strategy, 4, 1000));
}

test "exponential backoff caps at max_seconds" {
    const strategy = RetryStrategy{ .exponential = .{ .base_seconds = 10, .max_seconds = 100, .jitter = false } };
    // attempt 10: 10 * 2^10 = 10240, capped to 100
    try std.testing.expectEqual(@as(i64, 1100), nextRetryAt(strategy, 10, 1000));
}

test "exponential jitter stays within range" {
    const strategy = RetryStrategy{ .exponential = .{ .base_seconds = 10, .max_seconds = 3600, .jitter = true } };
    const result = nextRetryAt(strategy, 2, 1000);
    // base delay = 40, jitter up to 25% = 10, so result in [1040, 1050)
    try std.testing.expect(result >= 1040);
    try std.testing.expect(result < 1060);
}

test "linear backoff produces correct delays" {
    const strategy = RetryStrategy{ .linear = .{ .delay_seconds = 60 } };
    try std.testing.expectEqual(@as(i64, 1060), nextRetryAt(strategy, 0, 1000));
    try std.testing.expectEqual(@as(i64, 1120), nextRetryAt(strategy, 1, 1000));
    try std.testing.expectEqual(@as(i64, 1180), nextRetryAt(strategy, 2, 1000));
}

test "constant backoff produces same delay" {
    const strategy = RetryStrategy{ .constant = .{ .delay_seconds = 30 } };
    try std.testing.expectEqual(@as(i64, 1030), nextRetryAt(strategy, 0, 1000));
    try std.testing.expectEqual(@as(i64, 1030), nextRetryAt(strategy, 1, 1000));
    try std.testing.expectEqual(@as(i64, 1030), nextRetryAt(strategy, 5, 1000));
}
