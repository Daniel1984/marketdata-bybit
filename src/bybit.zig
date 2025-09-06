const std = @import("std");
const ws = @import("websocket");
const zimq = @import("zimq");
const types = @import("./types.zig");
const str = @import("./stream.zig");
const json = std.json;
const crypto = std.crypto;

pub const Self = @This();

allocator: std.mem.Allocator,
endpoint: []const u8,
ping_interval: u64,
ping_timeout: u64,
mutex: std.Thread.Mutex,
client: ?ws.Client,
stream: str.Self,

pub fn init(allocator: std.mem.Allocator, stream: str.Self) !Self {
    return Self{
        .allocator = allocator,
        .endpoint = "wss://stream.bybit.com/v5/public/spot",
        .ping_interval = 20000,
        .ping_timeout = 10000,
        .mutex = std.Thread.Mutex{},
        .client = null,
        .stream = stream,
    };
}

pub fn deinit(self: *Self) void {
    self.deinitClient();
}

fn deinitClient(self: *Self) void {
    if (self.client) |*client| {
        client.deinit();
        self.client = null;
    }
}

pub fn connectWebSocket(self: *Self) !void {
    const uri = try std.Uri.parse(self.endpoint);

    const host_component = uri.host.?;
    const host = switch (host_component) {
        .raw => |raw| raw,
        .percent_encoded => |encoded| encoded,
    };

    const port: u16 = uri.port orelse if (std.mem.eql(u8, uri.scheme, "wss")) 443 else 80;
    const is_tls = std.mem.eql(u8, uri.scheme, "wss");

    std.log.info("ws connection details: host={s}, port={}, tls={}", .{ host, port, is_tls });

    self.client = try ws.Client.init(self.allocator, .{
        .port = port,
        .host = host,
        .tls = is_tls,
        .max_size = 4096,
        .buffer_size = 1024,
    });

    const path = switch (uri.path) {
        .raw => |raw| raw,
        .percent_encoded => |encoded| encoded,
    };

    const headers = try std.fmt.allocPrint(self.allocator, "Host: {s}", .{host});
    defer self.allocator.free(headers);

    self.client.?.handshake(path, .{
        .timeout_ms = 10000,
        .headers = headers,
    }) catch |err| {
        std.log.err("ws handshake failed: {}", .{err});
        return err;
    };

    std.log.info("✓ ws connection and handshake successful!", .{});
}

pub fn subscribeChannel(self: *Self, topics: []const []const u8) !void {
    var args = try self.allocator.alloc([]const u8, topics.len);
    defer self.allocator.free(args);

    for (topics, 0..) |topic, i| {
        args[i] = topic;
    }

    const subscribe_msg = types.SubscribeMessage{ .op = "subscribe", .args = args };
    const subscribe_json = try std.fmt.allocPrint(self.allocator, "{f}", .{std.json.fmt(subscribe_msg, .{})});
    defer self.allocator.free(subscribe_json);

    std.log.info("subscription payload: {s}", .{subscribe_json});
    try self.client.?.write(subscribe_json);
}

pub fn consume(self: *Self) !void {
    try self.client.?.readTimeout(5000); // 5 second timeout
    var ping_timer = try std.time.Timer.start();
    const ping_interval_ns = self.ping_interval * std.time.ns_per_ms;

    while (true) {
        // check if we need to send ping
        if (ping_timer.read() >= ping_interval_ns) {
            const ping_msg = types.PingMessage{};
            const ping_json = try std.fmt.allocPrint(self.allocator, "{f}", .{std.json.fmt(ping_msg, .{})});
            defer self.allocator.free(ping_json);

            self.client.?.write(ping_json) catch |err| {
                std.log.err("failed to send ping: {}", .{err});
                return err; // exit consume loop on ping failure
            };
            std.log.info("sent ping", .{});
            ping_timer.reset();
        }

        const message = self.client.?.read() catch |err| {
            std.log.err("failed reading raw message: {}", .{err});
            return err; // exit consume loop on read failure
        };

        if (message) |msg| {
            defer self.client.?.done(msg);

            switch (msg.type) {
                .text => {
                    const parsed = std.json.parseFromSlice(std.json.Value, self.allocator, msg.data, .{}) catch |err| {
                        std.log.warn("Failed to parse message as JSON: {}", .{err});
                        continue;
                    };
                    defer parsed.deinit();

                    if (parsed.value.object.get("op")) |op_value| {
                        switch (op_value) {
                            .string => |op_str| {
                                if (std.mem.eql(u8, op_str, "subscribe")) {
                                    std.log.info("✓ Subscription acknowledged", .{});
                                    continue;
                                }

                                if (std.mem.eql(u8, op_str, "pong")) {
                                    std.log.info("✓ Pong received", .{});
                                    continue;
                                }
                            },
                            else => {},
                        }
                    }

                    if (parsed.value.object.get("topic")) |_| {
                        // Transform Bybit response data to match common format
                        const transformed_data = self.transformOrderbookData(msg.data) catch |err| {
                            std.log.warn("failed to transform orderbook data: {}", .{err});
                            continue;
                        };
                        defer self.allocator.free(transformed_data);

                        self.stream.publishMessage(transformed_data) catch |err| {
                            std.log.warn("failed publishing msg: {}", .{err});
                        };
                        continue;
                    }

                    if (parsed.value.object.get("ret_msg")) |ret_msg| {
                        switch (ret_msg) {
                            .string => |msg_str| {
                                if (std.mem.eql(u8, msg_str, "pong")) {
                                    std.log.info("✓ Pong received", .{});
                                    continue;
                                }
                            },
                            else => {},
                        }
                    }
                },
                .binary => {
                    std.log.info("Received binary message of {} bytes", .{msg.data.len});
                },
                .ping => {
                    std.log.info("Received ping", .{});
                    const pong_data = try self.allocator.dupe(u8, msg.data);
                    defer self.allocator.free(pong_data);
                    try self.client.?.writePong(pong_data);
                },
                .pong => {
                    std.log.info("Received pong", .{});
                },
                .close => {
                    std.log.info("WebSocket connection closed by server", .{});
                    try self.client.?.close(.{});
                    break;
                },
            }
        }
    }

    std.log.info("WebSocket connection closed", .{});
}

fn transformOrderbookData(self: *Self, original_data: []const u8) ![]u8 {
    const parsed = std.json.parseFromSlice(std.json.Value, self.allocator, original_data, .{}) catch |err| {
        std.log.warn("Failed to parse Bybit message for transformation: {}", .{err});
        return err;
    };
    defer parsed.deinit();

    // Clone the original structure
    var root_obj = std.json.ObjectMap.init(self.allocator);
    defer root_obj.deinit();

    try root_obj.put("src", std.json.Value{ .string = "bybit" });
    try root_obj.put("type", std.json.Value{ .string = "orderbook" });

    if (parsed.value.object.get("ts")) |ts| {
        try root_obj.put("ts", ts);
    }

    if (parsed.value.object.get("data")) |data_value| {
        var data_obj = std.json.ObjectMap.init(self.allocator);
        defer data_obj.deinit();

        if (data_value.object.get("s")) |s| {
            try root_obj.put("pair", s);
        }

        if (data_value.object.get("b")) |b| {
            try root_obj.put("bids", b);
        }

        if (data_value.object.get("a")) |a| {
            try root_obj.put("asks", a);
        }
    }

    const root_json_value = std.json.Value{ .object = root_obj };
    return try std.fmt.allocPrint(self.allocator, "{f}", .{std.json.fmt(root_json_value, .{})});
}
