package com.example.demo_gateway_ai.mcp;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Parsing du payload JSON-RPC 2.0 d'une requête MCP (EF-3), y compris le cas batch
 * (tableau JSON de messages). Ne lève jamais d'exception : tout échec de parsing
 * produit un {@link McpRequestInfo} marqué {@code parsed == false} (EF-4).
 */
final class JsonRpcRequestParser {

    /** Instance partagée, immuable et thread-safe (Jackson 3). */
    private static final ObjectMapper MAPPER = JsonMapper.builder().build();

    private JsonRpcRequestParser() {
    }

    static McpRequestInfo parse(byte[] payload, String sessionId) {
        if (payload == null || payload.length == 0) {
            return McpRequestInfo.headersOnly(sessionId);
        }
        JsonNode root;
        try {
            root = MAPPER.readTree(payload);
        } catch (JacksonException e) {
            return McpRequestInfo.unparsed(sessionId);
        }
        if (root.isObject()) {
            return fromSingle(root, sessionId);
        }
        if (root.isArray() && !root.isEmpty()) {
            return fromBatch(root, sessionId);
        }
        // scalaire, null ou tableau vide : JSON valide mais pas un message JSON-RPC
        return McpRequestInfo.unparsed(sessionId);
    }

    private static McpRequestInfo fromSingle(JsonNode node, String sessionId) {
        String method = text(node.path("method"));
        return new McpRequestInfo(
                method,
                method == null ? List.of() : List.of(method),
                text(node.path("id")),
                toolName(node, method),
                protocolVersion(node, method),
                sessionId,
                false,
                true);
    }

    private static McpRequestInfo fromBatch(JsonNode array, String sessionId) {
        List<String> methods = new ArrayList<>();
        String id = null;
        String toolName = null;
        String protocolVersion = null;
        for (JsonNode element : array) {
            String method = text(element.path("method"));
            if (method != null) {
                methods.add(method);
            }
            if (id == null) {
                id = text(element.path("id"));
            }
            if (toolName == null) {
                toolName = toolName(element, method);
            }
            if (protocolVersion == null) {
                protocolVersion = protocolVersion(element, method);
            }
        }
        String firstMethod = methods.isEmpty() ? null : methods.get(0);
        return new McpRequestInfo(firstMethod, methods, id, toolName, protocolVersion, sessionId, true, true);
    }

    private static String toolName(JsonNode node, String method) {
        return "tools/call".equals(method) ? text(node.path("params").path("name")) : null;
    }

    private static String protocolVersion(JsonNode node, String method) {
        return "initialize".equals(method) ? text(node.path("params").path("protocolVersion")) : null;
    }

    private static String text(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull() || node.isObject() || node.isArray()) {
            return null;
        }
        return node.asString(null);
    }
}
