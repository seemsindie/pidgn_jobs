const std = @import("std");
const job_mod = @import("job.zig");
const Job = job_mod.Job;

pub const JobResult = struct {
    job: Job,
    duration_ms: i64,
    error_msg: ?[]const u8,
};

pub const Event = union(enum) {
    job_enqueued: Job,
    job_started: Job,
    job_completed: JobResult,
    job_failed: JobResult,
    job_discarded: JobResult,
    queue_paused: []const u8,
    queue_resumed: []const u8,
};

pub const HandlerFn = *const fn (Event) void;

pub const Telemetry = struct {
    const max_handlers = 8;

    handlers: [max_handlers]HandlerFn = undefined,
    handler_count: usize = 0,
    mutex: std.Thread.Mutex = .{},

    pub fn attach(self: *Telemetry, handler: HandlerFn) void {
        while (!self.mutex.tryLock()) {}
        defer self.mutex.unlock();

        if (self.handler_count < max_handlers) {
            self.handlers[self.handler_count] = handler;
            self.handler_count += 1;
        }
    }

    pub fn emit(self: *Telemetry, event: Event) void {
        while (!self.mutex.tryLock()) {}
        defer self.mutex.unlock();

        for (0..self.handler_count) |i| {
            self.handlers[i](event);
        }
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

var test_event_count: usize = 0;
var test_last_event_tag: ?std.meta.Tag(Event) = null;

fn testHandler(event: Event) void {
    test_event_count += 1;
    test_last_event_tag = event;
}

test "telemetry handler receives events" {
    test_event_count = 0;
    test_last_event_tag = null;

    var telemetry = Telemetry{};
    telemetry.attach(&testHandler);

    const dummy_job = Job{
        .id = 1,
        .state = .available,
        .queue = "default",
        .worker = "test_worker",
        .args = "{}",
        .priority = 0,
        .attempt = 0,
        .max_attempts = 3,
        .scheduled_at = 0,
        .attempted_at = null,
        .completed_at = null,
        .inserted_at = 0,
        .errors = null,
        .unique_key = null,
    };

    telemetry.emit(.{ .job_enqueued = dummy_job });
    try std.testing.expectEqual(@as(usize, 1), test_event_count);
    try std.testing.expectEqual(std.meta.Tag(Event).job_enqueued, test_last_event_tag.?);

    telemetry.emit(.{ .job_completed = .{ .job = dummy_job, .duration_ms = 100, .error_msg = null } });
    try std.testing.expectEqual(@as(usize, 2), test_event_count);
    try std.testing.expectEqual(std.meta.Tag(Event).job_completed, test_last_event_tag.?);
}

test "multiple telemetry handlers" {
    test_event_count = 0;

    var telemetry = Telemetry{};
    telemetry.attach(&testHandler);
    telemetry.attach(&testHandler);

    const dummy_job = Job{
        .id = 1,
        .state = .available,
        .queue = "default",
        .worker = "test_worker",
        .args = "{}",
        .priority = 0,
        .attempt = 0,
        .max_attempts = 3,
        .scheduled_at = 0,
        .attempted_at = null,
        .completed_at = null,
        .inserted_at = 0,
        .errors = null,
        .unique_key = null,
    };

    telemetry.emit(.{ .job_enqueued = dummy_job });
    try std.testing.expectEqual(@as(usize, 2), test_event_count);
}
