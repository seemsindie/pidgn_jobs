const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    _ = b.standardOptimizeOption(.{});

    const sqlite_enabled = b.option(bool, "sqlite", "Enable SQLite support") orelse true;
    const postgres_enabled = b.option(bool, "postgres", "Enable PostgreSQL support") orelse false;

    // Create a module for the jobs build options so source can query at comptime
    // Include a package marker to avoid cache collision with zzz_db's db_options
    const jobs_options = b.addOptions();
    jobs_options.addOption([]const u8, "package", "zzz_jobs");
    jobs_options.addOption(bool, "sqlite_enabled", sqlite_enabled);
    jobs_options.addOption(bool, "postgres_enabled", postgres_enabled);

    // Import zzz_db dependency
    const zzz_db_dep = b.dependency("zzz_db", .{
        .target = target,
        .sqlite = sqlite_enabled,
        .postgres = postgres_enabled,
    });

    const mod = b.addModule("zzz_jobs", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
    });

    mod.addImport("jobs_options", jobs_options.createModule());
    mod.addImport("zzz_db", zzz_db_dep.module("zzz_db"));

    if (sqlite_enabled) {
        mod.link_libc = true;
    }

    if (postgres_enabled) {
        mod.link_libc = true;
    }

    const mod_tests = b.addTest(.{
        .root_module = mod,
    });

    const run_mod_tests = b.addRunArtifact(mod_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);
}
