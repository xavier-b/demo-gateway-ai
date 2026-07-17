package com.example.demo_gateway_ai.mcp;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** EF-3 / EF-4 : extraction JSON-RPC côté requête. */
class JsonRpcRequestParserTest {

    private static McpRequestInfo parse(String json) {
        return JsonRpcRequestParser.parse(json.getBytes(StandardCharsets.UTF_8), "sess-1");
    }

    @Test
    void toolsCall() {
        McpRequestInfo info = parse("""
                {"jsonrpc":"2.0","id":42,"method":"tools/call",
                 "params":{"name":"get_weather","arguments":{"city":"Paris"}}}""");
        assertTrue(info.parsed());
        assertEquals("tools/call", info.method());
        assertEquals("42", info.id());
        assertEquals("get_weather", info.toolName());
        assertEquals("sess-1", info.sessionId());
        assertFalse(info.batch());
        assertNull(info.protocolVersion());
    }

    @Test
    void notificationWithoutId() {
        McpRequestInfo info = parse("{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}");
        assertTrue(info.parsed());
        assertEquals("notifications/initialized", info.method());
        assertNull(info.id());
        assertNull(info.toolName());
    }

    @Test
    void initializeExposesProtocolVersion() {
        McpRequestInfo info = parse("""
                {"jsonrpc":"2.0","id":0,"method":"initialize",
                 "params":{"protocolVersion":"2025-03-26","capabilities":{}}}""");
        assertEquals("initialize", info.method());
        assertEquals("0", info.id());
        assertEquals("2025-03-26", info.protocolVersion());
    }

    @Test
    void batchExposesAllMethods() {
        McpRequestInfo info = parse("""
                [{"jsonrpc":"2.0","id":1,"method":"tools/list"},
                 {"jsonrpc":"2.0","method":"notifications/initialized"},
                 {"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_weather"}}]""");
        assertTrue(info.parsed());
        assertTrue(info.batch());
        assertEquals(List.of("tools/list", "notifications/initialized", "tools/call"), info.methods());
        assertEquals("tools/list", info.method());
        assertEquals("1", info.id());
        assertEquals("get_weather", info.toolName());
    }

    @Test
    void invalidJsonIsToleratedAsUnparsed() {
        McpRequestInfo info = parse("{not json at all");
        assertFalse(info.parsed());
        assertNull(info.method());
        assertEquals("sess-1", info.sessionId());
    }

    @Test
    void scalarJsonIsNotJsonRpc() {
        assertFalse(parse("42").parsed());
        assertFalse(parse("[]").parsed());
    }

    @Test
    void emptyPayloadIsHeadersOnly() {
        McpRequestInfo info = JsonRpcRequestParser.parse(new byte[0], "sess-9");
        assertTrue(info.parsed());
        assertNull(info.method());
        assertEquals("sess-9", info.sessionId());
    }
}
