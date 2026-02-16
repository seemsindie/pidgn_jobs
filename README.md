# zzz_jobs

A background job processing library for the zzz ecosystem. Provides job queues with flexible storage backends, retry strategies, cron scheduling, and telemetry.

## Features

- **Generic Store Pattern** -- same API for in-memory and database-backed queues
- **MemoryStore** -- zero-dependency in-process queue (great for testing and simple deployments)
- **DbStore** -- persistent job storage backed by SQLite or PostgreSQL via zzz_db
- **Supervisor** -- manages worker threads, polling, and graceful shutdown
- **Priority Queues** -- jobs processed by priority then insertion order
- **Retry Strategies** -- exponential, linear, constant backoff, or custom functions
- **Cron Scheduling** -- standard 5-field cron expressions with bitmask matching
- **Unique Jobs** -- idempotency keys with `ignore_new` or `cancel_existing` strategies
- **Telemetry** -- event-driven hooks for job lifecycle observability
- **Queue Control** -- pause/resume individual queues at runtime
- **Stuck Job Rescue** -- automatic recovery of timed-out jobs

## Quick Start

### In-Memory Jobs

```zig
const zzz_jobs = @import("zzz_jobs");

fn myWorker(args: []const u8, ctx: *zzz_jobs.JobContext) anyerror!void {
    // Process the job
    _ = args;
    _ = ctx;
}

var supervisor = try zzz_jobs.MemorySupervisor.init(.{}, .{
    .queues = &.{.{ .name = "default", .concurrency = 10 }},
    .poll_interval_ms = 1000,
});
defer supervisor.deinit();

supervisor.registerWorker(.{
    .name = "my_worker",
    .handler = &myWorker,
    .retry_strategy = .{ .exponential = .{} },
});

// Enqueue a job
_ = try supervisor.enqueue("my_worker", "{\"user_id\": 42}", .{
    .queue = "default",
    .priority = 0,
    .max_attempts = 5,
});

// Start processing
try supervisor.start();
// ... application runs ...
supervisor.stop();
```

### Database-Backed Jobs

```zig
// SQLite backend
var supervisor = try zzz_jobs.SqliteSupervisor.init(
    .{ .connection = .{} },
    .{ .queues = &.{.{ .name = "default", .concurrency = 5 }} },
);
```

### Cron Scheduling

```zig
try supervisor.registerCron(
    "nightly_cleanup",
    "0 2 * * *",       // 2:00 AM daily
    "cleanup_worker",
    "{}",
    .{},
);

try supervisor.registerCron(
    "every_fifteen",
    "*/15 * * * *",    // Every 15 minutes
    "sync_worker",
    "{}",
    .{},
);
```

### Retry Strategies

```zig
// Exponential backoff (default): 15s, 30s, 60s, ... up to 1 hour
.retry_strategy = .{ .exponential = .{
    .base_seconds = 15,
    .max_seconds = 3600,
    .jitter = true,
} }

// Linear: 60s, 120s, 180s, ...
.retry_strategy = .{ .linear = .{ .delay_seconds = 60 } }

// Constant: always 30s
.retry_strategy = .{ .constant = .{ .delay_seconds = 30 } }

// Custom function
.retry_strategy = .{ .custom = &myRetryFn }
```

### Unique Jobs

```zig
_ = try supervisor.enqueue("email_worker", "{\"to\": \"user@example.com\"}", .{
    .unique_key = "welcome-email-user-123",
    .unique_strategy = .ignore_new,  // Skip if already pending
});
```

### Telemetry

```zig
var telemetry = zzz_jobs.Telemetry{};
telemetry.attach(&fn(event: zzz_jobs.Event) void {
    switch (event) {
        .job_completed => |result| {
            log.info("Job completed in {}ms", .{result.duration_ms});
        },
        .job_failed => |result| {
            log.err("Job failed: {s}", .{result.error});
        },
        else => {},
    }
});
```

## Job States

```
available -> executing -> completed
                      \-> retryable -> available (retry)
                      \-> discarded (max attempts exceeded)
                      \-> cancelled

scheduled -> available (when scheduled_at arrives)
```

## Building

```bash
zig build          # Build (in-memory store only)
zig build test     # Run tests

# With SQLite backend
zig build -Dsqlite=true

# With PostgreSQL backend
zig build -Dpostgres=true
```

## Requirements

- Zig 0.16.0-dev.2535+b5bd49460 or later
- zzz_db (for database-backed stores)
- SQLite3 (optional, for SqliteDbStore)
- PostgreSQL (optional, for PgDbStore)

## License

MIT License - Copyright (c) 2026 Ivan Stamenkovic
