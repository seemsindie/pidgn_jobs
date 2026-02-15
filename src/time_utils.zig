const std = @import("std");

const c = @cImport({
    @cInclude("time.h");
});

pub fn timestamp() i64 {
    return c.time(null);
}

pub fn sleepMs(ms: u32) void {
    var ts: c.struct_timespec = .{
        .tv_sec = @intCast(@divTrunc(ms, 1000)),
        .tv_nsec = @intCast(@as(u64, @mod(ms, 1000)) * 1_000_000),
    };
    _ = c.nanosleep(&ts, &ts);
}

pub const DateTime = struct {
    year: u16,
    month: u4, // 1-12
    day: u5, // 1-31
    hour: u5, // 0-23
    minute: u6, // 0-59
    dow: u3, // 0=Sun, 1=Mon, ..., 6=Sat (cron convention)
};

pub fn fromTimestamp(ts: i64) DateTime {
    const epoch = std.time.epoch;
    const es = epoch.EpochSeconds{ .secs = @intCast(ts) };
    const day_secs = es.getDaySeconds();
    const epoch_day = es.getEpochDay();
    const year_day = epoch_day.calculateYearDay();
    const month_day = year_day.calculateMonthDay();

    // Compute day of week: Jan 1, 1970 was a Thursday (cron dow 4)
    // day 0 = Thursday, so (day + 4) % 7 gives cron dow (0=Sun)
    const day_num = epoch_day.day;
    const cron_dow: u3 = @intCast(@mod(day_num + 4, 7));

    return .{
        .year = year_day.year,
        .month = @intFromEnum(month_day.month),
        .day = month_day.day_index + 1,
        .hour = day_secs.getHoursIntoDay(),
        .minute = day_secs.getMinutesIntoHour(),
        .dow = cron_dow,
    };
}

test "timestamp returns positive value" {
    const t = timestamp();
    try std.testing.expect(t > 0);
}

test "fromTimestamp produces valid datetime" {
    // 2023-11-14T12:00:00 UTC = 1699963200
    const dt = fromTimestamp(1699963200);
    try std.testing.expectEqual(@as(u16, 2023), dt.year);
    try std.testing.expectEqual(@as(u4, 11), dt.month);
    try std.testing.expectEqual(@as(u5, 14), dt.day);
    try std.testing.expectEqual(@as(u5, 12), dt.hour);
    try std.testing.expectEqual(@as(u6, 0), dt.minute);
    // Nov 14 2023 was a Tuesday = cron dow 2
    try std.testing.expectEqual(@as(u3, 2), dt.dow);
}
