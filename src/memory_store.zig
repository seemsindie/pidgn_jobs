const std = @import("std");
const job_mod = @import("job.zig");
const retry_mod = @import("retry.zig");
const time_utils = @import("time_utils.zig");
const Job = job_mod.Job;
const JobState = job_mod.JobState;
const JobOpts = job_mod.JobOpts;
const RetryStrategy = retry_mod.RetryStrategy;

pub const MemoryStore = struct {
    const max_jobs = 4096;
    const max_paused = 16;

    pub const Config = struct {};

    jobs: [max_jobs]Job = undefined,
    job_count: usize = 0,
    next_id: i64 = 1,
    mutex: std.atomic.Mutex = .unlocked,
    paused_queues: [max_paused]PausedQueue = [_]PausedQueue{.{}} ** max_paused,
    paused_count: usize = 0,

    retry_strategies: [64]WorkerRetry = [_]WorkerRetry{.{}} ** 64,
    retry_count: usize = 0,

    const PausedQueue = struct {
        name: [64]u8 = [_]u8{0} ** 64,
        len: usize = 0,
    };

    const WorkerRetry = struct {
        name: [128]u8 = [_]u8{0} ** 128,
        name_len: usize = 0,
        strategy: RetryStrategy = .{ .exponential = .{} },
    };

    pub fn init(_: Config) !MemoryStore {
        return .{};
    }

    pub fn deinit(_: *MemoryStore) void {}

    pub fn registerRetryStrategy(self: *MemoryStore, worker_name: []const u8, strategy: RetryStrategy) void {
        while (!self.mutex.tryLock()) {}
        defer self.mutex.unlock();

        if (self.retry_count < 64) {
            const len = @min(worker_name.len, 128);
            @memcpy(self.retry_strategies[self.retry_count].name[0..len], worker_name[0..len]);
            self.retry_strategies[self.retry_count].name_len = len;
            self.retry_strategies[self.retry_count].strategy = strategy;
            self.retry_count += 1;
        }
    }

    fn getRetryStrategy(self: *MemoryStore, worker_name: []const u8) RetryStrategy {
        for (0..self.retry_count) |i| {
            const entry = &self.retry_strategies[i];
            if (entry.name_len == worker_name.len and
                std.mem.eql(u8, entry.name[0..entry.name_len], worker_name))
            {
                return entry.strategy;
            }
        }
        return .{ .exponential = .{} };
    }

    pub fn enqueue(self: *MemoryStore, worker: []const u8, args: []const u8, opts: JobOpts) !Job {
        while (!self.mutex.tryLock()) {}
        defer self.mutex.unlock();

        // Unique job check
        if (opts.unique_key) |key| {
            for (0..self.job_count) |i| {
                const existing = &self.jobs[i];
                if (existing.unique_key) |ek| {
                    if (std.mem.eql(u8, ek, key) and
                        existing.state != .completed and
                        existing.state != .discarded and
                        existing.state != .cancelled)
                    {
                        switch (opts.unique_strategy) {
                            .ignore_new => return existing.*,
                            .cancel_existing => {
                                existing.state = .discarded;
                                break;
                            },
                        }
                    }
                }
            }
        }

        if (self.job_count >= max_jobs) return error.StoreFull;

        const now = time_utils.timestamp();
        const job = Job{
            .id = self.next_id,
            .state = if (opts.scheduled_at != null and opts.scheduled_at.? > now) .scheduled else .available,
            .queue = opts.queue,
            .worker = worker,
            .args = args,
            .priority = opts.priority,
            .attempt = 0,
            .max_attempts = opts.max_attempts,
            .scheduled_at = opts.scheduled_at orelse now,
            .attempted_at = null,
            .completed_at = null,
            .inserted_at = now,
            .errors = null,
            .unique_key = opts.unique_key,
        };

        self.jobs[self.job_count] = job;
        self.job_count += 1;
        self.next_id += 1;

        return job;
    }

    pub fn claim(self: *MemoryStore, queue: []const u8) !?Job {
        while (!self.mutex.tryLock()) {}
        defer self.mutex.unlock();

        const now = time_utils.timestamp();

        // Promote scheduled jobs whose time has come
        for (0..self.job_count) |i| {
            if (self.jobs[i].state == .scheduled and self.jobs[i].scheduled_at <= now) {
                self.jobs[i].state = .available;
            }
        }

        // Find best candidate: available, matching queue, scheduled_at <= now
        // Priority: lowest priority value first, then oldest inserted_at (FIFO)
        var best_idx: ?usize = null;
        for (0..self.job_count) |i| {
            const job = &self.jobs[i];
            if (job.state != .available) continue;
            if (!std.mem.eql(u8, job.queue, queue)) continue;
            if (job.scheduled_at > now) continue;

            if (best_idx) |bi| {
                const best = &self.jobs[bi];
                if (job.priority < best.priority or
                    (job.priority == best.priority and job.inserted_at < best.inserted_at))
                {
                    best_idx = i;
                }
            } else {
                best_idx = i;
            }
        }

        if (best_idx) |idx| {
            self.jobs[idx].state = .executing;
            self.jobs[idx].attempted_at = now;
            self.jobs[idx].attempt += 1;
            return self.jobs[idx];
        }

        return null;
    }

    pub fn complete(self: *MemoryStore, job_id: i64) !void {
        while (!self.mutex.tryLock()) {}
        defer self.mutex.unlock();

        for (0..self.job_count) |i| {
            if (self.jobs[i].id == job_id) {
                self.jobs[i].state = .completed;
                self.jobs[i].completed_at = time_utils.timestamp();
                return;
            }
        }
        return error.JobNotFound;
    }

    pub fn fail(self: *MemoryStore, job_id: i64, error_msg: []const u8) !void {
        while (!self.mutex.tryLock()) {}
        defer self.mutex.unlock();

        for (0..self.job_count) |i| {
            if (self.jobs[i].id == job_id) {
                self.jobs[i].errors = error_msg;
                if (self.jobs[i].attempt >= self.jobs[i].max_attempts) {
                    self.jobs[i].state = .discarded;
                } else {
                    const strategy = self.getRetryStrategy(self.jobs[i].worker);
                    const now = time_utils.timestamp();
                    self.jobs[i].scheduled_at = retry_mod.nextRetryAt(strategy, self.jobs[i].attempt, now);
                    self.jobs[i].state = .available;
                }
                return;
            }
        }
        return error.JobNotFound;
    }

    pub fn discard(self: *MemoryStore, job_id: i64, error_msg: []const u8) !void {
        while (!self.mutex.tryLock()) {}
        defer self.mutex.unlock();

        for (0..self.job_count) |i| {
            if (self.jobs[i].id == job_id) {
                self.jobs[i].state = .discarded;
                self.jobs[i].errors = error_msg;
                return;
            }
        }
        return error.JobNotFound;
    }

    pub fn rescueStuck(self: *MemoryStore, timeout_seconds: i64) !u32 {
        while (!self.mutex.tryLock()) {}
        defer self.mutex.unlock();

        const now = time_utils.timestamp();
        var rescued: u32 = 0;

        for (0..self.job_count) |i| {
            if (self.jobs[i].state == .executing) {
                if (self.jobs[i].attempted_at) |at| {
                    if (now - at > timeout_seconds) {
                        self.jobs[i].state = .available;
                        self.jobs[i].errors = "rescued: job timed out";
                        rescued += 1;
                    }
                }
            }
        }

        return rescued;
    }

    pub fn countByState(self: *MemoryStore, queue: []const u8, state: JobState) !i64 {
        while (!self.mutex.tryLock()) {}
        defer self.mutex.unlock();

        var count: i64 = 0;
        for (0..self.job_count) |i| {
            if (self.jobs[i].state == state and std.mem.eql(u8, self.jobs[i].queue, queue)) {
                count += 1;
            }
        }
        return count;
    }

    pub fn pause(self: *MemoryStore, queue: []const u8) void {
        while (!self.mutex.tryLock()) {}
        defer self.mutex.unlock();

        if (self.paused_count >= max_paused) return;
        for (0..self.paused_count) |i| {
            if (self.paused_queues[i].len == queue.len and
                std.mem.eql(u8, self.paused_queues[i].name[0..self.paused_queues[i].len], queue))
            {
                return;
            }
        }
        const len = @min(queue.len, 64);
        @memcpy(self.paused_queues[self.paused_count].name[0..len], queue[0..len]);
        self.paused_queues[self.paused_count].len = len;
        self.paused_count += 1;
    }

    pub fn resume_queue(self: *MemoryStore, queue: []const u8) void {
        while (!self.mutex.tryLock()) {}
        defer self.mutex.unlock();

        for (0..self.paused_count) |i| {
            if (self.paused_queues[i].len == queue.len and
                std.mem.eql(u8, self.paused_queues[i].name[0..self.paused_queues[i].len], queue))
            {
                if (i < self.paused_count - 1) {
                    self.paused_queues[i] = self.paused_queues[self.paused_count - 1];
                }
                self.paused_queues[self.paused_count - 1] = .{};
                self.paused_count -= 1;
                return;
            }
        }
    }

    pub fn isPaused(self: *MemoryStore, queue: []const u8) bool {
        while (!self.mutex.tryLock()) {}
        defer self.mutex.unlock();

        for (0..self.paused_count) |i| {
            if (self.paused_queues[i].len == queue.len and
                std.mem.eql(u8, self.paused_queues[i].name[0..self.paused_queues[i].len], queue))
            {
                return true;
            }
        }
        return false;
    }

    pub fn deleteCompleted(self: *MemoryStore, older_than: i64) !u32 {
        while (!self.mutex.tryLock()) {}
        defer self.mutex.unlock();

        var deleted: u32 = 0;
        var i: usize = 0;
        while (i < self.job_count) {
            if (self.jobs[i].state == .completed) {
                if (self.jobs[i].completed_at) |cat| {
                    if (cat < older_than) {
                        if (i < self.job_count - 1) {
                            self.jobs[i] = self.jobs[self.job_count - 1];
                        }
                        self.job_count -= 1;
                        deleted += 1;
                        continue;
                    }
                }
            }
            i += 1;
        }

        return deleted;
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "enqueue creates job with correct defaults" {
    var store = try MemoryStore.init(.{});
    defer store.deinit();

    const job = try store.enqueue("my_worker", "{\"key\":\"value\"}", .{});
    try std.testing.expectEqual(@as(i64, 1), job.id);
    try std.testing.expectEqual(JobState.available, job.state);
    try std.testing.expectEqualStrings("default", job.queue);
    try std.testing.expectEqualStrings("my_worker", job.worker);
    try std.testing.expectEqual(@as(i32, 0), job.priority);
    try std.testing.expectEqual(@as(i32, 0), job.attempt);
    try std.testing.expectEqual(@as(i32, 20), job.max_attempts);
}

test "claim returns highest-priority first, then FIFO" {
    var store = try MemoryStore.init(.{});
    defer store.deinit();

    _ = try store.enqueue("w1", "a1", .{ .priority = 5 });
    _ = try store.enqueue("w2", "a2", .{ .priority = 1 });
    _ = try store.enqueue("w3", "a3", .{ .priority = 1 });

    const first = (try store.claim("default")).?;
    try std.testing.expectEqualStrings("w2", first.worker);
    try std.testing.expectEqualStrings("a2", first.args);

    const second = (try store.claim("default")).?;
    try std.testing.expectEqualStrings("w3", second.worker);

    const third = (try store.claim("default")).?;
    try std.testing.expectEqualStrings("w1", third.worker);
}

test "claim returns null on empty queue" {
    var store = try MemoryStore.init(.{});
    defer store.deinit();

    const result = try store.claim("default");
    try std.testing.expectEqual(@as(?Job, null), result);
}

test "claim skips future-scheduled jobs" {
    var store = try MemoryStore.init(.{});
    defer store.deinit();

    const future = time_utils.timestamp() + 999999;
    _ = try store.enqueue("w1", "args", .{ .scheduled_at = future });

    const result = try store.claim("default");
    try std.testing.expectEqual(@as(?Job, null), result);
}

test "complete sets state to completed" {
    var store = try MemoryStore.init(.{});
    defer store.deinit();

    const job = try store.enqueue("w1", "args", .{});
    _ = try store.claim("default");
    try store.complete(job.id);

    const count = try store.countByState("default", .completed);
    try std.testing.expectEqual(@as(i64, 1), count);
}

test "fail transitions to retryable then discarded after max_attempts" {
    var store = try MemoryStore.init(.{});
    defer store.deinit();

    _ = try store.enqueue("w1", "args", .{ .max_attempts = 2 });

    const job1 = (try store.claim("default")).?;
    try std.testing.expectEqual(@as(i32, 1), job1.attempt);
    try store.fail(job1.id, "error1");

    // Job should be available again (attempt < max_attempts)
    // But scheduled_at may be in the future due to retry delay
    // For test, we check it's not discarded yet
    const discarded1 = try store.countByState("default", .discarded);
    try std.testing.expectEqual(@as(i64, 0), discarded1);

    // Manually reset scheduled_at so we can claim it again
    {
        while (!store.mutex.tryLock()) {}
        defer store.mutex.unlock();
        store.jobs[0].scheduled_at = time_utils.timestamp() - 1;
    }

    const job2 = (try store.claim("default")).?;
    try std.testing.expectEqual(@as(i32, 2), job2.attempt);
    try store.fail(job2.id, "error2");

    const discarded = try store.countByState("default", .discarded);
    try std.testing.expectEqual(@as(i64, 1), discarded);
}

test "discard sets state to discarded" {
    var store = try MemoryStore.init(.{});
    defer store.deinit();

    const job = try store.enqueue("w1", "args", .{});
    _ = try store.claim("default");
    try store.discard(job.id, "manual discard");

    const count = try store.countByState("default", .discarded);
    try std.testing.expectEqual(@as(i64, 1), count);
}

test "rescueStuck reclaims timed-out jobs" {
    var store = try MemoryStore.init(.{});
    defer store.deinit();

    _ = try store.enqueue("w1", "args", .{});
    _ = try store.claim("default");

    // Manually set attempted_at to past to simulate timeout
    {
        while (!store.mutex.tryLock()) {}
        defer store.mutex.unlock();
        store.jobs[0].attempted_at = time_utils.timestamp() - 400;
    }

    const rescued = try store.rescueStuck(300);
    try std.testing.expectEqual(@as(u32, 1), rescued);

    const avail = try store.countByState("default", .available);
    try std.testing.expectEqual(@as(i64, 1), avail);
}

test "pause and resume queue" {
    var store = try MemoryStore.init(.{});
    defer store.deinit();

    try std.testing.expect(!store.isPaused("default"));
    store.pause("default");
    try std.testing.expect(store.isPaused("default"));
    store.resume_queue("default");
    try std.testing.expect(!store.isPaused("default"));
}

test "unique job ignore_new" {
    var store = try MemoryStore.init(.{});
    defer store.deinit();

    const job1 = try store.enqueue("w1", "args1", .{ .unique_key = "key1" });
    const job2 = try store.enqueue("w1", "args2", .{ .unique_key = "key1", .unique_strategy = .ignore_new });

    try std.testing.expectEqual(job1.id, job2.id);
    try std.testing.expectEqual(@as(usize, 1), store.job_count);
}

test "unique job cancel_existing" {
    var store = try MemoryStore.init(.{});
    defer store.deinit();

    const job1 = try store.enqueue("w1", "args1", .{ .unique_key = "key1" });
    const job2 = try store.enqueue("w1", "args2", .{ .unique_key = "key1", .unique_strategy = .cancel_existing });

    try std.testing.expect(job1.id != job2.id);
    try std.testing.expectEqual(@as(usize, 2), store.job_count);

    const discarded = try store.countByState("default", .discarded);
    try std.testing.expectEqual(@as(i64, 1), discarded);
}

test "countByState accuracy" {
    var store = try MemoryStore.init(.{});
    defer store.deinit();

    _ = try store.enqueue("w1", "a1", .{});
    _ = try store.enqueue("w2", "a2", .{});
    _ = try store.enqueue("w3", "a3", .{});

    try std.testing.expectEqual(@as(i64, 3), try store.countByState("default", .available));
    try std.testing.expectEqual(@as(i64, 0), try store.countByState("default", .executing));

    _ = try store.claim("default");
    try std.testing.expectEqual(@as(i64, 2), try store.countByState("default", .available));
    try std.testing.expectEqual(@as(i64, 1), try store.countByState("default", .executing));
}

test "deleteCompleted removes old completed jobs" {
    var store = try MemoryStore.init(.{});
    defer store.deinit();

    const job = try store.enqueue("w1", "args", .{});
    _ = try store.claim("default");
    try store.complete(job.id);

    const deleted = try store.deleteCompleted(time_utils.timestamp() + 999999);
    try std.testing.expectEqual(@as(u32, 1), deleted);
    try std.testing.expectEqual(@as(usize, 0), store.job_count);
}
