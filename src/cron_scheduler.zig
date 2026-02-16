const std = @import("std");
const cron_mod = @import("cron.zig");
const job_mod = @import("job.zig");
const store_mod = @import("store.zig");
const time_utils = @import("time_utils.zig");
const CronExpr = cron_mod.CronExpr;
const JobOpts = job_mod.JobOpts;

pub fn CronScheduler(comptime Store: type) type {
    comptime store_mod.validate(Store);

    return struct {
        const Self = @This();
        const max_entries = 32;

        const CronEntry = struct {
            name: [128]u8 = [_]u8{0} ** 128,
            name_len: usize = 0,
            cron_expr: CronExpr = undefined,
            worker: [128]u8 = [_]u8{0} ** 128,
            worker_len: usize = 0,
            args: [512]u8 = [_]u8{0} ** 512,
            args_len: usize = 0,
            opts: JobOpts = .{},
            last_run: i64 = 0,
        };

        entries: [max_entries]CronEntry = [_]CronEntry{.{}} ** max_entries,
        entry_count: usize = 0,
        store: *Store,
        running: *std.atomic.Value(bool),
        thread: ?std.Thread = null,
        tz_offset: i32 = 0,

        pub fn init(store: *Store, running: *std.atomic.Value(bool)) Self {
            return .{
                .store = store,
                .running = running,
            };
        }

        /// Initialize with a fixed timezone offset (seconds from UTC).
        /// Example: EST = -18000, JST = 32400.
        /// Note: Uses a fixed offset; does not account for DST transitions.
        pub fn initWithTimezone(store: *Store, running: *std.atomic.Value(bool), tz_offset: i32) Self {
            return .{
                .store = store,
                .running = running,
                .tz_offset = tz_offset,
            };
        }

        pub fn register(self: *Self, name: []const u8, cron_expr_str: []const u8, worker: []const u8, args: []const u8, opts: JobOpts) !void {
            if (self.entry_count >= max_entries) return error.TooManyCronEntries;

            const expr = try CronExpr.parse(cron_expr_str);
            var entry = &self.entries[self.entry_count];

            const name_len = @min(name.len, 128);
            @memcpy(entry.name[0..name_len], name[0..name_len]);
            entry.name_len = name_len;

            entry.cron_expr = expr;

            const worker_len = @min(worker.len, 128);
            @memcpy(entry.worker[0..worker_len], worker[0..worker_len]);
            entry.worker_len = worker_len;

            const args_len = @min(args.len, 512);
            @memcpy(entry.args[0..args_len], args[0..args_len]);
            entry.args_len = args_len;

            entry.opts = opts;
            entry.last_run = 0;

            self.entry_count += 1;
        }

        pub fn start(self: *Self) !void {
            if (self.entry_count > 0) {
                self.thread = try std.Thread.spawn(.{}, cronLoop, .{self});
            }
        }

        pub fn stop(self: *Self) void {
            if (self.thread) |t| {
                t.join();
                self.thread = null;
            }
        }

        fn cronLoop(self: *Self) void {
            while (self.running.load(.acquire)) {
                const now = time_utils.timestamp();
                const current_minute = now - @mod(now, 60);

                for (0..self.entry_count) |i| {
                    var entry = &self.entries[i];
                    if (entry.cron_expr.matchesWithOffset(current_minute, self.tz_offset) and entry.last_run != current_minute) {
                        entry.last_run = current_minute;
                        _ = self.store.enqueue(
                            entry.worker[0..entry.worker_len],
                            entry.args[0..entry.args_len],
                            entry.opts,
                        ) catch {};
                    }
                }

                // Sleep ~30 seconds, checking running flag periodically
                var slept: u32 = 0;
                while (slept < 30_000 and self.running.load(.acquire)) {
                    time_utils.sleepMs(500);
                    slept += 500;
                }
            }
        }
    };
}

// ── Tests ──────────────────────────────────────────────────────────────

const MemoryStore = @import("memory_store.zig").MemoryStore;

test "cron scheduler enqueues on match" {
    var store = try MemoryStore.init(.{});
    defer store.deinit();
    var running = std.atomic.Value(bool).init(true);

    var scheduler = CronScheduler(MemoryStore).init(&store, &running);
    try scheduler.register("every_minute", "* * * * *", "cron_worker", "{}", .{});

    try std.testing.expectEqual(@as(usize, 1), scheduler.entry_count);

    // Manually trigger what cronLoop does
    const now = time_utils.timestamp();
    const current_minute = now - @mod(now, 60);

    if (scheduler.entries[0].cron_expr.matches(current_minute)) {
        _ = try store.enqueue(
            scheduler.entries[0].worker[0..scheduler.entries[0].worker_len],
            scheduler.entries[0].args[0..scheduler.entries[0].args_len],
            scheduler.entries[0].opts,
        );
    }

    const count = try store.countByState("default", .available);
    try std.testing.expectEqual(@as(i64, 1), count);
}

test "cron scheduler initWithTimezone stores offset" {
    var store = try MemoryStore.init(.{});
    defer store.deinit();
    var running = std.atomic.Value(bool).init(true);

    const est_offset: i32 = -5 * 3600;
    var scheduler = CronScheduler(MemoryStore).initWithTimezone(&store, &running, est_offset);
    try std.testing.expectEqual(@as(i32, -18000), scheduler.tz_offset);

    try scheduler.register("every_minute", "* * * * *", "tz_worker", "{}", .{});
    try std.testing.expectEqual(@as(usize, 1), scheduler.entry_count);
}
