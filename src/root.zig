//! zzz_jobs - Background Job Processing for the Zzz Web Framework
//!
//! Provides background job processing with in-memory and database-backed stores,
//! configurable retry strategies, cron scheduling, unique jobs, and telemetry hooks.
//! All core types are generic over a store type (MemoryStore or DbStore).

const std = @import("std");
const zzz_db = @import("zzz_db");
const jobs_options = @import("jobs_options");

// Core types
pub const job = @import("job.zig");
pub const Job = job.Job;
pub const JobState = job.JobState;
pub const JobOpts = job.JobOpts;
pub const JobContext = job.JobContext;
pub const WorkerDef = job.WorkerDef;
pub const HandlerFn = job.HandlerFn;
pub const UniqueStrategy = job.UniqueStrategy;

// Store
pub const store = @import("store.zig");
pub const MemoryStore = @import("memory_store.zig").MemoryStore;
pub const DbStore = @import("db_store.zig").DbStore;

// Supervisor
pub const Supervisor = @import("supervisor.zig").Supervisor;
pub const QueueConfig = @import("supervisor.zig").QueueConfig;

// Retry
pub const retry = @import("retry.zig");
pub const RetryStrategy = retry.RetryStrategy;
pub const ExponentialBackoff = retry.ExponentialBackoff;
pub const LinearBackoff = retry.LinearBackoff;
pub const ConstantBackoff = retry.ConstantBackoff;

// Cron
pub const CronExpr = @import("cron.zig").CronExpr;
pub const CronScheduler = @import("cron_scheduler.zig").CronScheduler;

// Time utilities
pub const time_utils = @import("time_utils.zig");

// Telemetry
pub const telemetry = @import("telemetry.zig");
pub const Telemetry = telemetry.Telemetry;
pub const Event = telemetry.Event;
pub const JobResult = telemetry.JobResult;

// Convenience aliases
pub const MemorySupervisor = Supervisor(MemoryStore);

pub const SqliteDbStore = if (jobs_options.sqlite_enabled) DbStore(zzz_db.sqlite) else struct {};
pub const SqliteSupervisor = if (jobs_options.sqlite_enabled) Supervisor(SqliteDbStore) else struct {};

pub const PgDbStore = if (jobs_options.postgres_enabled) DbStore(zzz_db.postgres) else struct {};
pub const PgSupervisor = if (jobs_options.postgres_enabled) Supervisor(PgDbStore) else struct {};

pub const version = "0.1.0";

test {
    std.testing.refAllDecls(@This());
}
