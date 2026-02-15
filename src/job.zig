const std = @import("std");
const retry_mod = @import("retry.zig");
pub const RetryStrategy = retry_mod.RetryStrategy;

pub const JobState = enum(u8) {
    available = 0,
    executing = 1,
    completed = 2,
    retryable = 3,
    discarded = 4,
    cancelled = 5,
    scheduled = 6,
};

pub const Job = struct {
    id: i64,
    state: JobState,
    queue: []const u8,
    worker: []const u8,
    args: []const u8,
    priority: i32,
    attempt: i32,
    max_attempts: i32,
    scheduled_at: i64,
    attempted_at: ?i64,
    completed_at: ?i64,
    inserted_at: i64,
    errors: ?[]const u8,
    unique_key: ?[]const u8,
};

pub const JobOpts = struct {
    queue: []const u8 = "default",
    priority: i32 = 0,
    max_attempts: i32 = 20,
    scheduled_at: ?i64 = null,
    unique_key: ?[]const u8 = null,
    unique_strategy: UniqueStrategy = .ignore_new,
    timeout_seconds: i64 = 300,
};

pub const UniqueStrategy = enum { ignore_new, cancel_existing };

pub const JobContext = struct {
    job: Job,
    attempt: i32,
    cancelled: *const std.atomic.Value(bool),
};

pub const HandlerFn = *const fn ([]const u8, *JobContext) anyerror!void;

pub const WorkerDef = struct {
    name: []const u8,
    handler: HandlerFn,
    opts: JobOpts = .{},
    retry_strategy: RetryStrategy = .{ .exponential = .{} },
};

// ── Tests ──────────────────────────────────────────────────────────────

test "JobState values" {
    try std.testing.expectEqual(@as(u8, 0), @intFromEnum(JobState.available));
    try std.testing.expectEqual(@as(u8, 1), @intFromEnum(JobState.executing));
    try std.testing.expectEqual(@as(u8, 2), @intFromEnum(JobState.completed));
    try std.testing.expectEqual(@as(u8, 3), @intFromEnum(JobState.retryable));
    try std.testing.expectEqual(@as(u8, 4), @intFromEnum(JobState.discarded));
    try std.testing.expectEqual(@as(u8, 5), @intFromEnum(JobState.cancelled));
    try std.testing.expectEqual(@as(u8, 6), @intFromEnum(JobState.scheduled));
}

test "JobOpts defaults" {
    const opts = JobOpts{};
    try std.testing.expectEqualStrings("default", opts.queue);
    try std.testing.expectEqual(@as(i32, 0), opts.priority);
    try std.testing.expectEqual(@as(i32, 20), opts.max_attempts);
    try std.testing.expectEqual(@as(?i64, null), opts.scheduled_at);
    try std.testing.expectEqual(@as(?[]const u8, null), opts.unique_key);
    try std.testing.expectEqual(@as(i64, 300), opts.timeout_seconds);
}
