pub const SubscribeMessage = struct {
    op: []const u8,
    args: [][]const u8,
};

pub const PingMessage = struct {
    req_id: ?[]const u8 = null,
    op: []const u8 = "ping",
};

pub const MessageEnvelope = struct {
    type: []const u8,
    source: []const u8,
    data: []const u8,
};
