const std = @import("std");
const job_mod = @import("job.zig");

/// Comptime validation that a store type has all required declarations.
pub fn validate(comptime Store: type) void {
    if (!@hasDecl(Store, "Config")) {
        @compileError("Store missing 'Config' type");
    }

    const required_methods = .{
        "init",
        "deinit",
        "enqueue",
        "claim",
        "complete",
        "fail",
        "discard",
        "rescueStuck",
        "countByState",
        "pause",
        "resume_queue",
        "isPaused",
        "deleteCompleted",
    };

    inline for (required_methods) |method| {
        if (!@hasDecl(Store, method)) {
            @compileError("Store missing '" ++ method ++ "' method");
        }
    }
}

test "validate accepts MemoryStore" {
    const MemoryStore = @import("memory_store.zig").MemoryStore;
    validate(MemoryStore);
}
