const std = @import("std");
const store_mod = @import("store.zig");
const job_mod = @import("job.zig");
const telemetry_mod = @import("telemetry.zig");
const cron_scheduler_mod = @import("cron_scheduler.zig");
const time_utils = @import("time_utils.zig");
const Job = job_mod.Job;
const JobState = job_mod.JobState;
const JobOpts = job_mod.JobOpts;
const JobContext = job_mod.JobContext;
const WorkerDef = job_mod.WorkerDef;
const HandlerFn = job_mod.HandlerFn;
const Telemetry = telemetry_mod.Telemetry;

pub const QueueConfig = struct {
    name: []const u8 = "default",
    concurrency: u8 = 10,
};

pub fn Supervisor(comptime Store: type) type {
    comptime store_mod.validate(Store);

    return struct {
        const Self = @This();
        const max_worker_defs = 64;
        const max_threads = 512;

        pub const Config = struct {
            queues: []const QueueConfig = &.{.{}},
            poll_interval_ms: u32 = 1000,
            rescue_interval_ms: u32 = 60_000,
            shutdown_timeout_ms: u32 = 30_000,
        };

        store: Store,
        config: Config,
        running: std.atomic.Value(bool),
        worker_defs: [max_worker_defs]WorkerDef = undefined,
        worker_def_count: usize = 0,
        threads: [max_threads]std.Thread = undefined,
        thread_count: usize = 0,
        rescue_thread: ?std.Thread = null,
        telemetry: ?*Telemetry = null,
        cron_scheduler: cron_scheduler_mod.CronScheduler(Store),

        pub fn init(store_config: Store.Config, config: Config) !Self {
            var store = try Store.init(store_config);
            return .{
                .store = store,
                .config = config,
                .running = std.atomic.Value(bool).init(false),
                .cron_scheduler = cron_scheduler_mod.CronScheduler(Store).init(&store, undefined),
            };
        }

        pub fn deinit(self: *Self) void {
            if (self.running.load(.acquire)) {
                self.stop();
            }
            self.store.deinit();
        }

        pub fn registerWorker(self: *Self, def: WorkerDef) void {
            if (self.worker_def_count < max_worker_defs) {
                self.worker_defs[self.worker_def_count] = def;
                self.worker_def_count += 1;
                self.store.registerRetryStrategy(def.name, def.retry_strategy);
            }
        }

        pub fn enqueue(self: *Self, worker_name: []const u8, args: []const u8, opts: JobOpts) !Job {
            const job = try self.store.enqueue(worker_name, args, opts);
            if (self.telemetry) |t| {
                t.emit(.{ .job_enqueued = job });
            }
            return job;
        }

        pub fn start(self: *Self) !void {
            self.running.store(true, .release);

            // Fix up cron_scheduler pointer after self is stable
            self.cron_scheduler.store = &self.store;
            self.cron_scheduler.running = &self.running;

            // Spawn worker threads per queue
            for (self.config.queues) |queue_cfg| {
                for (0..queue_cfg.concurrency) |_| {
                    if (self.thread_count >= max_threads) break;
                    self.threads[self.thread_count] = try std.Thread.spawn(.{}, workerLoop, .{ self, queue_cfg.name });
                    self.thread_count += 1;
                }
            }

            // Spawn rescue thread
            self.rescue_thread = try std.Thread.spawn(.{}, rescueLoop, .{self});

            // Start cron scheduler
            try self.cron_scheduler.start();
        }

        pub fn stop(self: *Self) void {
            self.running.store(false, .release);

            // Stop cron scheduler
            self.cron_scheduler.stop();

            // Join all worker threads
            for (0..self.thread_count) |i| {
                self.threads[i].join();
            }
            self.thread_count = 0;

            // Join rescue thread
            if (self.rescue_thread) |t| {
                t.join();
                self.rescue_thread = null;
            }
        }

        pub fn pauseQueue(self: *Self, queue: []const u8) void {
            self.store.pause(queue);
            if (self.telemetry) |t| {
                t.emit(.{ .queue_paused = queue });
            }
        }

        pub fn resumeQueue(self: *Self, queue: []const u8) void {
            self.store.resume_queue(queue);
            if (self.telemetry) |t| {
                t.emit(.{ .queue_resumed = queue });
            }
        }

        pub fn registerCron(self: *Self, name: []const u8, cron_expr: []const u8, worker: []const u8, args: []const u8, opts: JobOpts) !void {
            try self.cron_scheduler.register(name, cron_expr, worker, args, opts);
        }

        fn workerLoop(self: *Self, queue_name: []const u8) void {
            while (self.running.load(.acquire)) {
                if (self.store.isPaused(queue_name)) {
                    time_utils.sleepMs(self.config.poll_interval_ms);
                    continue;
                }

                const maybe_job = self.store.claim(queue_name) catch {
                    time_utils.sleepMs(self.config.poll_interval_ms);
                    continue;
                };

                if (maybe_job) |job| {
                    const handler = self.findHandler(job.worker);
                    if (handler) |h| {
                        const start_time = time_utils.timestamp();
                        var cancelled = std.atomic.Value(bool).init(false);
                        var ctx = JobContext{
                            .job = job,
                            .attempt = job.attempt,
                            .cancelled = &cancelled,
                        };

                        if (self.telemetry) |t| {
                            t.emit(.{ .job_started = job });
                        }

                        h(job.args, &ctx) catch |err| {
                            const duration = (time_utils.timestamp() - start_time) * 1000;
                            self.store.fail(job.id, @errorName(err)) catch {};
                            if (self.telemetry) |t| {
                                if (job.attempt >= job.max_attempts) {
                                    t.emit(.{ .job_discarded = .{ .job = job, .duration_ms = duration, .error_msg = @errorName(err) } });
                                } else {
                                    t.emit(.{ .job_failed = .{ .job = job, .duration_ms = duration, .error_msg = @errorName(err) } });
                                }
                            }
                            continue;
                        };

                        const duration = (time_utils.timestamp() - start_time) * 1000;
                        self.store.complete(job.id) catch {};
                        if (self.telemetry) |t| {
                            t.emit(.{ .job_completed = .{ .job = job, .duration_ms = duration, .error_msg = null } });
                        }
                    } else {
                        self.store.discard(job.id, "no handler registered") catch {};
                    }
                } else {
                    time_utils.sleepMs(self.config.poll_interval_ms);
                }
            }
        }

        fn rescueLoop(self: *Self) void {
            while (self.running.load(.acquire)) {
                _ = self.store.rescueStuck(300) catch {};
                time_utils.sleepMs(self.config.rescue_interval_ms);
            }
        }

        fn findHandler(self: *Self, name: []const u8) ?HandlerFn {
            for (0..self.worker_def_count) |i| {
                if (std.mem.eql(u8, self.worker_defs[i].name, name)) {
                    return self.worker_defs[i].handler;
                }
            }
            return null;
        }
    };
}

// ── Tests ──────────────────────────────────────────────────────────────

const MemoryStore = @import("memory_store.zig").MemoryStore;

var test_job_executed: bool = false;
var test_job_args: [256]u8 = undefined;
var test_job_args_len: usize = 0;

fn testWorkerHandler(args: []const u8, _: *JobContext) anyerror!void {
    test_job_executed = true;
    const len = @min(args.len, 256);
    @memcpy(test_job_args[0..len], args[0..len]);
    test_job_args_len = len;
}

fn failingHandler(_: []const u8, _: *JobContext) anyerror!void {
    return error.TestFailure;
}

test "supervisor start/stop with worker execution" {
    test_job_executed = false;
    test_job_args_len = 0;

    var supervisor = try Supervisor(MemoryStore).init(.{}, .{
        .queues = &.{.{ .name = "default", .concurrency = 1 }},
        .poll_interval_ms = 10,
        .rescue_interval_ms = 60_000,
    });
    defer supervisor.deinit();

    supervisor.registerWorker(.{
        .name = "test_worker",
        .handler = &testWorkerHandler,
    });

    _ = try supervisor.enqueue("test_worker", "hello", .{});

    try supervisor.start();
    time_utils.sleepMs(100);
    supervisor.stop();

    try std.testing.expect(test_job_executed);
    try std.testing.expectEqualStrings("hello", test_job_args[0..test_job_args_len]);
}

test "supervisor worker retry uses configured strategy" {
    var supervisor = try Supervisor(MemoryStore).init(.{}, .{
        .queues = &.{.{ .name = "default", .concurrency = 1 }},
        .poll_interval_ms = 10,
        .rescue_interval_ms = 60_000,
    });
    defer supervisor.deinit();

    supervisor.registerWorker(.{
        .name = "failing_worker",
        .handler = &failingHandler,
        .opts = .{ .max_attempts = 2 },
        .retry_strategy = .{ .constant = .{ .delay_seconds = 0 } },
    });

    _ = try supervisor.enqueue("failing_worker", "data", .{ .max_attempts = 2 });

    try supervisor.start();
    time_utils.sleepMs(200);
    supervisor.stop();

    // After max_attempts, job should be discarded
    const discarded = try supervisor.store.countByState("default", .discarded);
    try std.testing.expectEqual(@as(i64, 1), discarded);
}
