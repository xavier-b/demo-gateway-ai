package com.example.demo_gateway_ai.mcp;

import java.util.List;

/**
 * Informations extraites d'une requête MCP (EF-3). Record immuable porté par les
 * attributs d'exchange et transmis aux {@link McpEventListener}.
 *
 * @param method          méthode JSON-RPC (premier message en cas de batch), {@code null} si absente
 * @param methods         liste des méthodes (taille &gt; 1 uniquement en batch)
 * @param id              identifiant JSON-RPC de corrélation ({@code null} pour une notification)
 * @param toolName        {@code $.params.name} lorsque {@code method == tools/call}
 * @param protocolVersion {@code $.params.protocolVersion} lorsque {@code method == initialize}
 * @param sessionId       header {@code Mcp-Session-Id} de la requête
 * @param batch           {@code true} si le payload était un tableau JSON de messages
 * @param parsed          {@code false} si le payload n'a pas pu être interprété comme JSON-RPC (EF-4)
 */
public record McpRequestInfo(
        String method,
        List<String> methods,
        String id,
        String toolName,
        String protocolVersion,
        String sessionId,
        boolean batch,
        boolean parsed) {

    public McpRequestInfo {
        methods = methods == null ? List.of() : List.copyOf(methods);
    }

    /** Requête observée par ses seuls headers/URI (GET d'ouverture SSE, DELETE de session, corps vide). */
    public static McpRequestInfo headersOnly(String sessionId) {
        return new McpRequestInfo(null, List.of(), null, null, null, sessionId, false, true);
    }

    /** Payload présent mais impossible à interpréter comme JSON-RPC (EF-4). */
    public static McpRequestInfo unparsed(String sessionId) {
        return new McpRequestInfo(null, List.of(), null, null, null, sessionId, false, false);
    }
}
