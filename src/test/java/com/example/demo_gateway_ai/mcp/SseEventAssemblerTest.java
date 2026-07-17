package com.example.demo_gateway_ai.mcp;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** EF-6 : framing SSE + extraction JSON-RPC incrémentale, y compris coupures de chunk. */
class SseEventAssemblerTest {

    private static final String RESULT_EVENT =
            "data: {\"jsonrpc\":\"2.0\",\"id\":\"1\",\"result\":{\"content\":[{\"type\":\"text\"}]}}\n\n";
    private static final String ERROR_EVENT =
            "data: {\"jsonrpc\":\"2.0\",\"id\":\"3\",\"error\":{\"code\":-32601,\"message\":\"not found\"}}\n\n";

    private static List<McpResponseMessage> feed(String stream, int chunkSize) {
        return feed(stream, chunkSize, 16 * 1024);
    }

    private static List<McpResponseMessage> feed(String stream, int chunkSize, long maxEventBytes) {
        List<McpResponseMessage> messages = new ArrayList<>();
        SseEventAssembler assembler = new SseEventAssembler(maxEventBytes, messages::add);
        byte[] bytes = stream.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < bytes.length; i += chunkSize) {
            assembler.onChunk(bytes, i, Math.min(chunkSize, bytes.length - i));
        }
        assembler.close();
        return messages;
    }

    @Test
    void singleEventInOneChunk() {
        List<McpResponseMessage> messages = feed(RESULT_EVENT, Integer.MAX_VALUE);
        assertEquals(1, messages.size());
        McpResponseMessage message = messages.get(0);
        assertEquals("1", message.id());
        assertTrue(message.hasResult());
        assertFalse(message.hasError());
    }

    /** 5bis : évènement coupé à cheval sur plusieurs chunks, y compris au milieu d'une ligne data et du JSON. */
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 5, 7, 11, 16})
    void eventSplitAcrossChunksAtEveryBoundary(int chunkSize) {
        List<McpResponseMessage> messages = feed(RESULT_EVENT + ERROR_EVENT, chunkSize);
        assertEquals(2, messages.size());
        assertEquals("1", messages.get(0).id());
        assertTrue(messages.get(0).hasResult());
        assertEquals("3", messages.get(1).id());
        assertTrue(messages.get(1).hasError());
        assertEquals(-32601, messages.get(1).errorCode());
        assertEquals("not found", messages.get(1).errorMessage());
    }

    @Test
    void crlfLineEndings() {
        List<McpResponseMessage> messages =
                feed("data: {\"jsonrpc\":\"2.0\",\"id\":\"7\",\"result\":1}\r\n\r\n", 3);
        assertEquals(1, messages.size());
        assertEquals("7", messages.get(0).id());
        assertTrue(messages.get(0).hasResult());
    }

    @Test
    void jsonSpreadOverMultipleDataLines() {
        // les lignes data: successives sont jointes par \n (spec SSE)
        List<McpResponseMessage> messages =
                feed("data: {\"id\":\"8\",\ndata: \"result\": {\"a\": 1}}\n\n", 4);
        assertEquals(1, messages.size());
        assertEquals("8", messages.get(0).id());
        assertTrue(messages.get(0).hasResult());
    }

    @Test
    void commentsAndOtherFieldsAreIgnored() {
        List<McpResponseMessage> messages =
                feed(": keep-alive\nevent: message\nid: 42\nretry: 5\ndata: {\"id\":\"9\",\"result\":true}\n\n", 5);
        assertEquals(1, messages.size());
        assertEquals("9", messages.get(0).id());
    }

    @Test
    void oversizedEventIsAbandonedAndStreamContinues() {
        String big = "data: {\"id\":\"big\",\"result\":\"" + "x".repeat(200) + "\"}\n\n";
        String normal = "data: {\"jsonrpc\":\"2.0\",\"id\":\"ok\",\"result\":{}}\n\n";
        List<McpResponseMessage> messages = feed(big + normal, 7, 64);
        assertEquals(1, messages.size());
        assertEquals("ok", messages.get(0).id());
    }

    @Test
    void nonJsonDataIsToleratedAndNextEventStillParsed() {
        List<McpResponseMessage> messages = feed("data: hello world\n\n" + RESULT_EVENT, 6);
        assertEquals(1, messages.size());
        assertEquals("1", messages.get(0).id());
    }

    @Test
    void truncatedJsonAtEndOfEventYieldsNoMessage() {
        List<McpResponseMessage> messages = feed("data: {\"id\":\"x\",\"result\"\n\n" + RESULT_EVENT, 8);
        assertEquals(1, messages.size());
        assertEquals("1", messages.get(0).id());
    }

    @Test
    void nullResultStillCountsAsResult() {
        List<McpResponseMessage> messages = feed("data: {\"jsonrpc\":\"2.0\",\"id\":\"5\",\"result\":null}\n\n", 9);
        assertEquals(1, messages.size());
        assertTrue(messages.get(0).hasResult());
        assertFalse(messages.get(0).hasError());
        assertNull(messages.get(0).errorCode());
    }
}
