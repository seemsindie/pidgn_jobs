const std = @import("std");
const pidgn_db = @import("pidgn_db");
const job_mod = @import("job.zig");
const retry_mod = @import("retry.zig");
const time_utils = @import("time_utils.zig");
const Job = job_mod.Job;
const JobState = job_mod.JobState;
const JobOpts = job_mod.JobOpts;
const RetryStrategy = retry_mod.RetryStrategy;

fn copySlice(dest: []u8, pos: usize, src: []const u8) usize {
    const end = pos + src.len;
    if (end > dest.len) return 0;
    @memcpy(dest[pos..end], src);
    return src.len;
}

pub fn DbStore(comptime Backend: type) type {
    pidgn_db.backend.validate(Backend);

    return struct {
        const Self = @This();
        const max_paused = 16;

        pool: *pidgn_db.Pool(Backend),
        paused_queues: [max_paused]PausedQueue = [_]PausedQueue{.{}} ** max_paused,
        paused_count: usize = 0,
        mutex: std.atomic.Mutex = .unlocked,

        retry_strategies: [64]WorkerRetry = [_]WorkerRetry{.{}} ** 64,
        retry_count: usize = 0,

        // String buffers for claimed job data (SQLite returns pointers into statement memory
        // which become dangling after ResultSet.deinit)
        claimed_queue: [64]u8 = [_]u8{0} ** 64,
        claimed_worker: [128]u8 = [_]u8{0} ** 128,
        claimed_args: [4096]u8 = [_]u8{0} ** 4096,
        claimed_errors: [512]u8 = [_]u8{0} ** 512,
        claimed_unique_key: [128]u8 = [_]u8{0} ** 128,
        claimed_queue_len: usize = 0,
        claimed_worker_len: usize = 0,
        claimed_args_len: usize = 0,
        claimed_errors_len: usize = 0,
        claimed_unique_key_len: usize = 0,
        claimed_has_errors: bool = false,
        claimed_has_unique_key: bool = false,

        const PausedQueue = struct {
            name: [64]u8 = [_]u8{0} ** 64,
            len: usize = 0,
        };

        const WorkerRetry = struct {
            name: [128]u8 = [_]u8{0} ** 128,
            name_len: usize = 0,
            strategy: RetryStrategy = .{ .exponential = .{} },
        };

        pub const Config = struct {
            pool: *pidgn_db.Pool(Backend),
        };

        pub fn init(config: Config) !Self {
            var self = Self{
                .pool = config.pool,
            };
            try self.ensureTable();
            return self;
        }

        pub fn deinit(_: *Self) void {}

        pub fn registerRetryStrategy(self: *Self, worker_name: []const u8, strategy: RetryStrategy) void {
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

        fn getRetryStrategy(self: *Self, worker_name: []const u8) RetryStrategy {
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

        fn ensureTable(self: *Self) !void {
            var pc = try self.pool.checkout();
            defer pc.release();

            if (Backend.dialect == .postgres) {
                try pc.conn.exec(
                    \\CREATE TABLE IF NOT EXISTS pidgn_jobs (
                    \\  id BIGSERIAL PRIMARY KEY,
                    \\  state INTEGER NOT NULL DEFAULT 0,
                    \\  queue TEXT NOT NULL DEFAULT 'default',
                    \\  worker TEXT NOT NULL,
                    \\  args TEXT NOT NULL DEFAULT '',
                    \\  priority INTEGER NOT NULL DEFAULT 0,
                    \\  attempt INTEGER NOT NULL DEFAULT 0,
                    \\  max_attempts INTEGER NOT NULL DEFAULT 20,
                    \\  scheduled_at BIGINT NOT NULL DEFAULT 0,
                    \\  attempted_at BIGINT,
                    \\  completed_at BIGINT,
                    \\  inserted_at BIGINT NOT NULL DEFAULT 0,
                    \\  errors TEXT,
                    \\  unique_key TEXT
                    \\)
                );
            } else {
                try pc.conn.exec(
                    \\CREATE TABLE IF NOT EXISTS pidgn_jobs (
                    \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
                    \\  state INTEGER NOT NULL DEFAULT 0,
                    \\  queue TEXT NOT NULL DEFAULT 'default',
                    \\  worker TEXT NOT NULL,
                    \\  args TEXT NOT NULL DEFAULT '',
                    \\  priority INTEGER NOT NULL DEFAULT 0,
                    \\  attempt INTEGER NOT NULL DEFAULT 0,
                    \\  max_attempts INTEGER NOT NULL DEFAULT 20,
                    \\  scheduled_at BIGINT NOT NULL DEFAULT 0,
                    \\  attempted_at BIGINT,
                    \\  completed_at BIGINT,
                    \\  inserted_at BIGINT NOT NULL DEFAULT 0,
                    \\  errors TEXT,
                    \\  unique_key TEXT
                    \\)
                );
            }

            // Create index for efficient claiming
            try pc.conn.exec("CREATE INDEX IF NOT EXISTS idx_pidgn_jobs_claim ON pidgn_jobs (queue, state, scheduled_at, priority)");
        }

        pub fn enqueue(self: *Self, worker: []const u8, args: []const u8, opts: JobOpts) !Job {
            const now = time_utils.timestamp();

            // Handle unique jobs
            if (opts.unique_key) |key| {
                var pc = try self.pool.checkout();
                defer pc.release();

                var check_buf: [512]u8 = undefined;
                const check_sql = std.fmt.bufPrint(&check_buf, "SELECT id FROM pidgn_jobs WHERE unique_key = '{s}' AND state < 2 LIMIT 1", .{key}) catch return error.InternalError;
                check_buf[check_sql.len] = 0;
                const check_z: [:0]const u8 = check_buf[0..check_sql.len :0];

                var rs = try Backend.ResultSet.query(&pc.conn.db, check_z, &.{});
                defer rs.deinit();

                if (try rs.next()) {
                    const existing_id = rs.columnInt64(0);
                    switch (opts.unique_strategy) {
                        .ignore_new => {
                            return self.getJobById(existing_id) orelse return error.JobNotFound;
                        },
                        .cancel_existing => {
                            var discard_buf: [256]u8 = undefined;
                            const discard_sql = std.fmt.bufPrint(&discard_buf, "UPDATE pidgn_jobs SET state = 4 WHERE id = {d}", .{existing_id}) catch return error.InternalError;
                            discard_buf[discard_sql.len] = 0;
                            const discard_z: [:0]const u8 = discard_buf[0..discard_sql.len :0];
                            try pc.conn.exec(discard_z);
                        },
                    }
                }
            }

            const sched = opts.scheduled_at orelse now;
            const state: u8 = if (opts.scheduled_at != null and opts.scheduled_at.? > now) 6 else 0;

            var pc = try self.pool.checkout();
            defer pc.release();

            var buf: [2048]u8 = undefined;
            var pos: usize = 0;

            pos += copySlice(&buf, pos, "INSERT INTO pidgn_jobs (state, queue, worker, args, priority, attempt, max_attempts, scheduled_at, inserted_at");
            if (opts.unique_key != null) {
                pos += copySlice(&buf, pos, ", unique_key");
            }
            pos += copySlice(&buf, pos, ") VALUES (");

            const state_str = std.fmt.bufPrint(buf[pos..], "{d}", .{state}) catch return error.InternalError;
            pos += state_str.len;
            pos += copySlice(&buf, pos, ", '");
            pos += copySlice(&buf, pos, opts.queue);
            pos += copySlice(&buf, pos, "', '");
            pos += copySlice(&buf, pos, worker);
            pos += copySlice(&buf, pos, "', '");
            pos += copySlice(&buf, pos, args);
            pos += copySlice(&buf, pos, "', ");

            const prio_str = std.fmt.bufPrint(buf[pos..], "{d}", .{opts.priority}) catch return error.InternalError;
            pos += prio_str.len;
            pos += copySlice(&buf, pos, ", 0, ");

            const max_str = std.fmt.bufPrint(buf[pos..], "{d}", .{opts.max_attempts}) catch return error.InternalError;
            pos += max_str.len;
            pos += copySlice(&buf, pos, ", ");

            const sched_str = std.fmt.bufPrint(buf[pos..], "{d}", .{sched}) catch return error.InternalError;
            pos += sched_str.len;
            pos += copySlice(&buf, pos, ", ");

            const ins_str = std.fmt.bufPrint(buf[pos..], "{d}", .{now}) catch return error.InternalError;
            pos += ins_str.len;

            if (opts.unique_key) |key| {
                pos += copySlice(&buf, pos, ", '");
                pos += copySlice(&buf, pos, key);
                pos += copySlice(&buf, pos, "'");
            }

            pos += copySlice(&buf, pos, ")");
            buf[pos] = 0;
            const sql_z: [:0]const u8 = buf[0..pos :0];

            const exec_result = try Backend.ExecResult.exec(&pc.conn.db, sql_z, &.{});
            const new_id = exec_result.lastInsertId();

            return Job{
                .id = new_id,
                .state = @enumFromInt(state),
                .queue = opts.queue,
                .worker = worker,
                .args = args,
                .priority = opts.priority,
                .attempt = 0,
                .max_attempts = opts.max_attempts,
                .scheduled_at = sched,
                .attempted_at = null,
                .completed_at = null,
                .inserted_at = now,
                .errors = null,
                .unique_key = opts.unique_key,
            };
        }

        pub fn claim(self: *Self, queue: []const u8) !?Job {
            while (!self.mutex.tryLock()) {}
            defer self.mutex.unlock();

            var pc = try self.pool.checkout();
            defer pc.release();

            const now = time_utils.timestamp();

            // Promote scheduled jobs
            {
                var promo_buf: [256]u8 = undefined;
                const promo_sql = std.fmt.bufPrint(&promo_buf, "UPDATE pidgn_jobs SET state = 0 WHERE state = 6 AND scheduled_at <= {d}", .{now}) catch return error.InternalError;
                promo_buf[promo_sql.len] = 0;
                const promo_z: [:0]const u8 = promo_buf[0..promo_sql.len :0];
                try pc.conn.exec(promo_z);
            }

            // Find best candidate
            var find_buf: [512]u8 = undefined;
            var fpos: usize = 0;
            fpos += copySlice(&find_buf, fpos, "SELECT id, state, queue, worker, args, priority, attempt, max_attempts, scheduled_at, attempted_at, completed_at, inserted_at, errors, unique_key FROM pidgn_jobs WHERE queue = '");
            fpos += copySlice(&find_buf, fpos, queue);
            fpos += copySlice(&find_buf, fpos, "' AND state = 0 AND scheduled_at <= ");
            const now_str = std.fmt.bufPrint(find_buf[fpos..], "{d}", .{now}) catch return error.InternalError;
            fpos += now_str.len;
            fpos += copySlice(&find_buf, fpos, " ORDER BY priority, id LIMIT 1");
            find_buf[fpos] = 0;
            const find_z: [:0]const u8 = find_buf[0..fpos :0];

            var rs = try Backend.ResultSet.query(&pc.conn.db, find_z, &.{});
            defer rs.deinit();

            if (try rs.next()) {
                const job_id = rs.columnInt64(0);

                // Update to executing
                var upd_buf: [256]u8 = undefined;
                const upd_sql = std.fmt.bufPrint(&upd_buf, "UPDATE pidgn_jobs SET state = 1, attempted_at = {d}, attempt = attempt + 1 WHERE id = {d}", .{ now, job_id }) catch return error.InternalError;
                upd_buf[upd_sql.len] = 0;
                const upd_z: [:0]const u8 = upd_buf[0..upd_sql.len :0];
                try pc.conn.exec(upd_z);

                // Copy strings into stable buffers (SQLite column pointers are only valid
                // while the ResultSet is alive)
                const queue_len = @min(queue.len, self.claimed_queue.len);
                @memcpy(self.claimed_queue[0..queue_len], queue[0..queue_len]);
                self.claimed_queue_len = queue_len;

                if (rs.columnText(3)) |w| {
                    const wl = @min(w.len, self.claimed_worker.len);
                    @memcpy(self.claimed_worker[0..wl], w[0..wl]);
                    self.claimed_worker_len = wl;
                } else {
                    self.claimed_worker_len = 0;
                }

                if (rs.columnText(4)) |a| {
                    const al = @min(a.len, self.claimed_args.len);
                    @memcpy(self.claimed_args[0..al], a[0..al]);
                    self.claimed_args_len = al;
                } else {
                    self.claimed_args_len = 0;
                }

                if (rs.columnText(12)) |e| {
                    const el = @min(e.len, self.claimed_errors.len);
                    @memcpy(self.claimed_errors[0..el], e[0..el]);
                    self.claimed_errors_len = el;
                    self.claimed_has_errors = true;
                } else {
                    self.claimed_has_errors = false;
                }

                if (rs.columnText(13)) |uk| {
                    const ukl = @min(uk.len, self.claimed_unique_key.len);
                    @memcpy(self.claimed_unique_key[0..ukl], uk[0..ukl]);
                    self.claimed_unique_key_len = ukl;
                    self.claimed_has_unique_key = true;
                } else {
                    self.claimed_has_unique_key = false;
                }

                return Job{
                    .id = job_id,
                    .state = .executing,
                    .queue = self.claimed_queue[0..self.claimed_queue_len],
                    .worker = self.claimed_worker[0..self.claimed_worker_len],
                    .args = self.claimed_args[0..self.claimed_args_len],
                    .priority = @intCast(rs.columnInt64(5)),
                    .attempt = @intCast(rs.columnInt64(6) + 1),
                    .max_attempts = @intCast(rs.columnInt64(7)),
                    .scheduled_at = rs.columnInt64(8),
                    .attempted_at = now,
                    .completed_at = null,
                    .inserted_at = rs.columnInt64(11),
                    .errors = if (self.claimed_has_errors) self.claimed_errors[0..self.claimed_errors_len] else null,
                    .unique_key = if (self.claimed_has_unique_key) self.claimed_unique_key[0..self.claimed_unique_key_len] else null,
                };
            }

            return null;
        }

        pub fn complete(self: *Self, job_id: i64) !void {
            var pc = try self.pool.checkout();
            defer pc.release();

            const now = time_utils.timestamp();
            var buf: [256]u8 = undefined;
            const sql = std.fmt.bufPrint(&buf, "UPDATE pidgn_jobs SET state = 2, completed_at = {d} WHERE id = {d}", .{ now, job_id }) catch return error.InternalError;
            buf[sql.len] = 0;
            const sql_z: [:0]const u8 = buf[0..sql.len :0];
            try pc.conn.exec(sql_z);
        }

        pub fn fail(self: *Self, job_id: i64, error_msg: []const u8) !void {
            var pc = try self.pool.checkout();
            defer pc.release();

            // Get current attempt and max_attempts
            var get_buf: [256]u8 = undefined;
            const get_sql = std.fmt.bufPrint(&get_buf, "SELECT attempt, max_attempts, worker FROM pidgn_jobs WHERE id = {d}", .{job_id}) catch return error.InternalError;
            get_buf[get_sql.len] = 0;
            const get_z: [:0]const u8 = get_buf[0..get_sql.len :0];

            var rs = try Backend.ResultSet.query(&pc.conn.db, get_z, &.{});
            defer rs.deinit();

            if (try rs.next()) {
                const attempt: i32 = @intCast(rs.columnInt64(0));
                const max_attempts: i32 = @intCast(rs.columnInt64(1));
                const worker_name = rs.columnText(2) orelse "";

                if (attempt >= max_attempts) {
                    var buf: [512]u8 = undefined;
                    var pos: usize = 0;
                    pos += copySlice(&buf, pos, "UPDATE pidgn_jobs SET state = 4, errors = '");
                    pos += copySlice(&buf, pos, error_msg);
                    pos += copySlice(&buf, pos, "' WHERE id = ");
                    const id_str = std.fmt.bufPrint(buf[pos..], "{d}", .{job_id}) catch return error.InternalError;
                    pos += id_str.len;
                    buf[pos] = 0;
                    const sql_z: [:0]const u8 = buf[0..pos :0];
                    try pc.conn.exec(sql_z);
                } else {
                    const strategy = self.getRetryStrategy(worker_name);
                    const now = time_utils.timestamp();
                    const next_at = retry_mod.nextRetryAt(strategy, attempt, now);

                    var buf: [512]u8 = undefined;
                    const sql = std.fmt.bufPrint(&buf, "UPDATE pidgn_jobs SET state = 0, errors = '{s}', scheduled_at = {d} WHERE id = {d}", .{ error_msg, next_at, job_id }) catch return error.InternalError;
                    buf[sql.len] = 0;
                    const sql_z: [:0]const u8 = buf[0..sql.len :0];
                    try pc.conn.exec(sql_z);
                }
            }
        }

        pub fn discard(self: *Self, job_id: i64, error_msg: []const u8) !void {
            var pc = try self.pool.checkout();
            defer pc.release();

            var buf: [512]u8 = undefined;
            var pos: usize = 0;
            pos += copySlice(&buf, pos, "UPDATE pidgn_jobs SET state = 4, errors = '");
            pos += copySlice(&buf, pos, error_msg);
            pos += copySlice(&buf, pos, "' WHERE id = ");
            const id_str = std.fmt.bufPrint(buf[pos..], "{d}", .{job_id}) catch return error.InternalError;
            pos += id_str.len;
            buf[pos] = 0;
            const sql_z: [:0]const u8 = buf[0..pos :0];
            try pc.conn.exec(sql_z);
        }

        pub fn rescueStuck(self: *Self, timeout_seconds: i64) !u32 {
            var pc = try self.pool.checkout();
            defer pc.release();

            const now = time_utils.timestamp();
            const cutoff = now - timeout_seconds;

            var buf: [256]u8 = undefined;
            const sql = std.fmt.bufPrint(&buf, "UPDATE pidgn_jobs SET state = 0, errors = 'rescued: job timed out' WHERE state = 1 AND attempted_at < {d}", .{cutoff}) catch return error.InternalError;
            buf[sql.len] = 0;
            const sql_z: [:0]const u8 = buf[0..sql.len :0];

            const exec_result = try Backend.ExecResult.exec(&pc.conn.db, sql_z, &.{});
            return @intCast(exec_result.rowsAffected());
        }

        pub fn countByState(self: *Self, queue: []const u8, state: JobState) !i64 {
            var pc = try self.pool.checkout();
            defer pc.release();

            var buf: [256]u8 = undefined;
            var pos: usize = 0;
            pos += copySlice(&buf, pos, "SELECT COUNT(*) FROM pidgn_jobs WHERE queue = '");
            pos += copySlice(&buf, pos, queue);
            pos += copySlice(&buf, pos, "' AND state = ");
            const state_str = std.fmt.bufPrint(buf[pos..], "{d}", .{@intFromEnum(state)}) catch return error.InternalError;
            pos += state_str.len;
            buf[pos] = 0;
            const sql_z: [:0]const u8 = buf[0..pos :0];

            var rs = try Backend.ResultSet.query(&pc.conn.db, sql_z, &.{});
            defer rs.deinit();

            if (try rs.next()) {
                return rs.columnInt64(0);
            }
            return 0;
        }

        pub fn pause(self: *Self, queue: []const u8) void {
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

        pub fn resume_queue(self: *Self, queue: []const u8) void {
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

        pub fn isPaused(self: *Self, queue: []const u8) bool {
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

        pub fn deleteCompleted(self: *Self, older_than: i64) !u32 {
            var pc = try self.pool.checkout();
            defer pc.release();

            var buf: [256]u8 = undefined;
            const sql = std.fmt.bufPrint(&buf, "DELETE FROM pidgn_jobs WHERE state = 2 AND completed_at < {d}", .{older_than}) catch return error.InternalError;
            buf[sql.len] = 0;
            const sql_z: [:0]const u8 = buf[0..sql.len :0];

            const exec_result = try Backend.ExecResult.exec(&pc.conn.db, sql_z, &.{});
            return @intCast(exec_result.rowsAffected());
        }

        fn getJobById(self: *Self, job_id: i64) ?Job {
            var pc = self.pool.checkout() catch return null;
            defer pc.release();

            var buf: [512]u8 = undefined;
            const sql = std.fmt.bufPrint(&buf, "SELECT id, state, queue, worker, args, priority, attempt, max_attempts, scheduled_at, attempted_at, completed_at, inserted_at, errors, unique_key FROM pidgn_jobs WHERE id = {d}", .{job_id}) catch return null;
            buf[sql.len] = 0;
            const sql_z: [:0]const u8 = buf[0..sql.len :0];

            var rs = Backend.ResultSet.query(&pc.conn.db, sql_z, &.{}) catch return null;
            defer rs.deinit();

            const has_row = rs.next() catch return null;
            if (has_row) {
                return Job{
                    .id = rs.columnInt64(0),
                    .state = @enumFromInt(@as(u8, @intCast(rs.columnInt64(1)))),
                    .queue = rs.columnText(2) orelse "default",
                    .worker = rs.columnText(3) orelse "",
                    .args = rs.columnText(4) orelse "",
                    .priority = @intCast(rs.columnInt64(5)),
                    .attempt = @intCast(rs.columnInt64(6)),
                    .max_attempts = @intCast(rs.columnInt64(7)),
                    .scheduled_at = rs.columnInt64(8),
                    .attempted_at = if (rs.columnIsNull(9)) null else rs.columnInt64(9),
                    .completed_at = if (rs.columnIsNull(10)) null else rs.columnInt64(10),
                    .inserted_at = rs.columnInt64(11),
                    .errors = rs.columnText(12),
                    .unique_key = rs.columnText(13),
                };
            }
            return null;
        }
    };
}

// ── Tests ──────────────────────────────────────────────────────────────

const jobs_options = @import("jobs_options");
const sqlite = pidgn_db.sqlite;
const SqlitePool = pidgn_db.Pool(sqlite);
const SqliteDbStore = DbStore(sqlite);
const supervisor_mod = @import("supervisor.zig");
const Supervisor = supervisor_mod.Supervisor;

// Use a temp file-based SQLite so all pool connections share the same database
// (in-memory SQLite creates separate databases per connection without URI support)
const test_db_path = "/tmp/pidgn_jobs_test.db";
const test_db_config = sqlite.Config{
    .database = test_db_path,
    .enable_wal = false,
};

fn cleanTestDb() void {
    // Remove any leftover test database files
    const c_unlink = @cImport({ @cInclude("unistd.h"); });
    _ = c_unlink.unlink(test_db_path);
    _ = c_unlink.unlink(test_db_path ++ "-wal");
    _ = c_unlink.unlink(test_db_path ++ "-shm");
    _ = c_unlink.unlink(test_db_path ++ "-journal");
}

test "table auto-creation on init" {
    if (!jobs_options.sqlite_enabled) return;
    cleanTestDb();

    var pool = try SqlitePool.init(.{ .size = 1, .connection = test_db_config });
    defer pool.deinit();

    var store = try SqliteDbStore.init(.{ .pool = &pool });
    defer store.deinit();

    // Verify table exists by inserting
    var pc = try pool.checkout();
    defer pc.release();
    try pc.conn.exec("INSERT INTO pidgn_jobs (worker) VALUES ('test')");
}

test "enqueue and claim roundtrip" {
    if (!jobs_options.sqlite_enabled) return;
    cleanTestDb();

    var pool = try SqlitePool.init(.{ .size = 1, .connection = test_db_config });
    defer pool.deinit();

    var store = try SqliteDbStore.init(.{ .pool = &pool });
    defer store.deinit();

    const job = try store.enqueue("my_worker", "my_args", .{});
    try std.testing.expectEqualStrings("my_worker", job.worker);

    const claimed = (try store.claim("default")).?;
    try std.testing.expectEqual(job.id, claimed.id);
    try std.testing.expectEqual(JobState.executing, claimed.state);
}

test "complete/fail/discard state transitions" {
    if (!jobs_options.sqlite_enabled) return;
    cleanTestDb();

    var pool = try SqlitePool.init(.{ .size = 1, .connection = test_db_config });
    defer pool.deinit();

    var store = try SqliteDbStore.init(.{ .pool = &pool });
    defer store.deinit();

    // Test complete
    const j1 = try store.enqueue("w1", "a1", .{});
    _ = try store.claim("default");
    try store.complete(j1.id);
    try std.testing.expectEqual(@as(i64, 1), try store.countByState("default", .completed));

    // Test discard
    const j2 = try store.enqueue("w2", "a2", .{});
    _ = try store.claim("default");
    try store.discard(j2.id, "manual");
    try std.testing.expectEqual(@as(i64, 1), try store.countByState("default", .discarded));
}

test "rescueStuck reclaims timed-out jobs via db" {
    if (!jobs_options.sqlite_enabled) return;
    cleanTestDb();

    var pool = try SqlitePool.init(.{ .size = 1, .connection = test_db_config });
    defer pool.deinit();

    var store = try SqliteDbStore.init(.{ .pool = &pool });
    defer store.deinit();

    _ = try store.enqueue("w1", "args", .{});
    _ = try store.claim("default");

    // Manually set attempted_at to past
    {
        var pc = try pool.checkout();
        defer pc.release();
        var buf: [256]u8 = undefined;
        const past = time_utils.timestamp() - 400;
        const sql = std.fmt.bufPrint(&buf, "UPDATE pidgn_jobs SET attempted_at = {d} WHERE state = 1", .{past}) catch unreachable;
        buf[sql.len] = 0;
        const sql_z: [:0]const u8 = buf[0..sql.len :0];
        try pc.conn.exec(sql_z);
    }

    const rescued = try store.rescueStuck(300);
    try std.testing.expectEqual(@as(u32, 1), rescued);
}

var db_test_executed: bool = false;

fn dbTestHandler(_: []const u8, _: *job_mod.JobContext) anyerror!void {
    db_test_executed = true;
}

test "supervisor with DbStore(sqlite) end-to-end" {
    if (!jobs_options.sqlite_enabled) return;
    cleanTestDb();

    db_test_executed = false;

    // The Supervisor creates its own DbStore internally via Store.init(store_config).
    // We need to pass the pool that the Supervisor's store will use.
    // Use pool size 3 to have enough for concurrent worker + rescue threads.
    var pool = try SqlitePool.init(.{ .size = 3, .connection = test_db_config });
    defer pool.deinit();

    var supervisor = try Supervisor(SqliteDbStore).init(.{ .pool = &pool }, .{
        .queues = &.{.{ .name = "default", .concurrency = 1 }},
        .poll_interval_ms = 10,
        .rescue_interval_ms = 60_000,
    });
    defer supervisor.deinit();

    supervisor.registerWorker(.{
        .name = "db_worker",
        .handler = &dbTestHandler,
    });

    _ = try supervisor.enqueue("db_worker", "test_data", .{});

    // Verify the job is visible in the store before starting threads
    const count = try supervisor.store.countByState("default", .available);
    try std.testing.expectEqual(@as(i64, 1), count);

    try supervisor.start();
    time_utils.sleepMs(1000);
    supervisor.stop();

    try std.testing.expect(db_test_executed);
}
