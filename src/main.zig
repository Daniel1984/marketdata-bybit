const std = @import("std");
const bybit = @import("./bybit.zig");
const stream = @import("./stream.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var publisher = try stream.init(allocator, .{});
    defer publisher.deinit();
    try publisher.connect();

    var bb = try bybit.init(allocator, publisher);
    defer bb.deinit();

    try bb.connectWebSocket();
    const topics = [_][]const u8{"orderbook.1.BTCUSDT"};
    try bb.subscribeChannel(topics[0..]);
    try bb.consume();

    std.log.info("consumer stopped...", .{});
}
