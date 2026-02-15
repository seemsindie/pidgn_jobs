const std = @import("std");
const time_utils = @import("time_utils.zig");

pub const CronExpr = struct {
    minute: u64, // bitmask for 0-59
    hour: u32, // bitmask for 0-23
    dom: u32, // bitmask for 1-31
    month: u16, // bitmask for 1-12
    dow: u8, // bitmask for 0-6 (Sun=0)

    pub fn parse(expr: []const u8) !CronExpr {
        var result = CronExpr{
            .minute = 0,
            .hour = 0,
            .dom = 0,
            .month = 0,
            .dow = 0,
        };

        // Split into 5 fields
        var fields: [5][]const u8 = undefined;
        var field_count: usize = 0;
        var start: usize = 0;
        var in_space = true;

        for (expr, 0..) |ch, idx| {
            if (ch == ' ' or ch == '\t') {
                if (!in_space and field_count < 5) {
                    fields[field_count] = expr[start..idx];
                    field_count += 1;
                }
                in_space = true;
            } else {
                if (in_space) {
                    start = idx;
                    in_space = false;
                }
            }
        }
        if (!in_space and field_count < 5) {
            fields[field_count] = expr[start..];
            field_count += 1;
        }

        if (field_count != 5) return error.InvalidCronExpression;

        result.minute = try parseField(fields[0], 0, 59);
        result.hour = @intCast(try parseField(fields[1], 0, 23));
        result.dom = @intCast(try parseField(fields[2], 1, 31));
        result.month = @intCast(try parseField(fields[3], 1, 12));
        result.dow = @intCast(try parseField(fields[4], 0, 6));

        return result;
    }

    pub fn matches(self: CronExpr, ts: i64) bool {
        const dt = time_utils.fromTimestamp(ts);

        if ((self.minute & (@as(u64, 1) << @intCast(dt.minute))) == 0) return false;
        if ((self.hour & (@as(u32, 1) << @intCast(dt.hour))) == 0) return false;
        if ((self.dom & (@as(u32, 1) << @intCast(dt.day))) == 0) return false;
        if ((self.month & (@as(u16, 1) << @intCast(dt.month))) == 0) return false;
        if ((self.dow & (@as(u8, 1) << dt.dow)) == 0) return false;

        return true;
    }

    pub fn nextAfter(self: CronExpr, after: i64) !i64 {
        // Start from the next minute after 'after'
        var ts = after - @mod(after, 60) + 60;

        // Cap search at ~366 days
        const max_ts = after + 366 * 24 * 60 * 60;

        while (ts <= max_ts) {
            if (self.matches(ts)) return ts;
            ts += 60;
        }

        return error.NoMatchFound;
    }
};

fn parseField(field: []const u8, min: u6, max: u6) !u64 {
    var result: u64 = 0;

    // Split by commas
    var comma_start: usize = 0;
    var idx: usize = 0;
    while (idx <= field.len) {
        if (idx == field.len or field[idx] == ',') {
            const part = field[comma_start..idx];
            result |= try parsePart(part, min, max);
            comma_start = idx + 1;
        }
        idx += 1;
    }

    return result;
}

fn parsePart(part: []const u8, min: u6, max: u6) !u64 {
    // Check for step: */step or range/step
    var range_part = part;
    var step: u6 = 1;

    for (part, 0..) |ch, i| {
        if (ch == '/') {
            range_part = part[0..i];
            step = std.fmt.parseInt(u6, part[i + 1 ..], 10) catch return error.InvalidCronExpression;
            if (step == 0) return error.InvalidCronExpression;
            break;
        }
    }

    var range_min: u6 = min;
    var range_max: u6 = max;

    if (range_part.len == 1 and range_part[0] == '*') {
        // Wildcard: all values
    } else {
        // Check for range: N-M
        var has_dash = false;
        for (range_part, 0..) |ch, i| {
            if (ch == '-') {
                range_min = std.fmt.parseInt(u6, range_part[0..i], 10) catch return error.InvalidCronExpression;
                range_max = std.fmt.parseInt(u6, range_part[i + 1 ..], 10) catch return error.InvalidCronExpression;
                has_dash = true;
                break;
            }
        }

        if (!has_dash) {
            // Single value
            const val = std.fmt.parseInt(u6, range_part, 10) catch return error.InvalidCronExpression;
            if (val < min or val > max) return error.InvalidCronExpression;
            return @as(u64, 1) << val;
        }
    }

    if (range_min < min or range_max > max or range_min > range_max) {
        return error.InvalidCronExpression;
    }

    var result: u64 = 0;
    var v: u6 = range_min;
    while (v <= range_max) : (v += step) {
        result |= @as(u64, 1) << v;
        if (v == max) break; // Prevent overflow
    }

    return result;
}

// ── Tests ──────────────────────────────────────────────────────────────

test "parse every minute" {
    const expr = try CronExpr.parse("* * * * *");
    for (0..60) |i| {
        const bit: u6 = @intCast(i);
        try std.testing.expect((expr.minute & (@as(u64, 1) << bit)) != 0);
    }
}

test "parse */15 * * * *" {
    const expr = try CronExpr.parse("*/15 * * * *");
    try std.testing.expect((expr.minute & (@as(u64, 1) << 0)) != 0);
    try std.testing.expect((expr.minute & (@as(u64, 1) << 15)) != 0);
    try std.testing.expect((expr.minute & (@as(u64, 1) << 30)) != 0);
    try std.testing.expect((expr.minute & (@as(u64, 1) << 45)) != 0);
    try std.testing.expect((expr.minute & (@as(u64, 1) << 1)) == 0);
}

test "parse 0 9 * * 1-5" {
    const expr = try CronExpr.parse("0 9 * * 1-5");
    try std.testing.expect((expr.minute & (@as(u64, 1) << 0)) != 0);
    try std.testing.expect((expr.minute & (@as(u64, 1) << 1)) == 0);
    try std.testing.expect((expr.hour & (@as(u32, 1) << 9)) != 0);
    try std.testing.expect((expr.hour & (@as(u32, 1) << 10)) == 0);
    try std.testing.expect((expr.dow & (@as(u8, 1) << 1)) != 0);
    try std.testing.expect((expr.dow & (@as(u8, 1) << 5)) != 0);
    try std.testing.expect((expr.dow & (@as(u8, 1) << 0)) == 0);
    try std.testing.expect((expr.dow & (@as(u8, 1) << 6)) == 0);
}

test "matches correct for known timestamps" {
    const expr = try CronExpr.parse("* * * * *");
    try std.testing.expect(expr.matches(1700000000));
    try std.testing.expect(expr.matches(1000000000));
}

test "nextAfter computes correct next occurrence" {
    const expr = try CronExpr.parse("* * * * *");
    const next = try expr.nextAfter(1700000000);
    try std.testing.expect(next > 1700000000);
    try std.testing.expectEqual(@as(i64, 0), @mod(next, 60));
}

test "invalid expression returns error" {
    const result = CronExpr.parse("invalid");
    try std.testing.expectError(error.InvalidCronExpression, result);

    const result2 = CronExpr.parse("* * *");
    try std.testing.expectError(error.InvalidCronExpression, result2);

    const result3 = CronExpr.parse("99 * * * *");
    try std.testing.expectError(error.InvalidCronExpression, result3);
}
