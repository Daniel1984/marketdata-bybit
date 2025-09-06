pub const SubscribeMessage = struct {
    op: []const u8,
    args: [][]const u8,
};

pub const PingMessage = struct {
    req_id: ?[]const u8 = null,
    op: []const u8 = "ping",
};
