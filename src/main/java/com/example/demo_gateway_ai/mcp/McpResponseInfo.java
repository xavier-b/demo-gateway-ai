package com.example.demo_gateway_ai.mcp;

/**
 * Synthèse d'un flux de réponse MCP, publiée à la terminaison du flux (EF-6).
 *
 * @param statusCode     code HTTP de la réponse
 * @param contentType    {@code Content-Type} de la réponse ({@code application/json} ou {@code text/event-stream})
 * @param sessionId      header {@code Mcp-Session-Id} retourné par le serveur (ex. lors de l'{@code initialize})
 * @param byteCount      nombre total d'octets écrits
 * @param chunkCount     nombre de chunks écrits
 * @param durationMillis durée totale du flux
 * @param sse            {@code true} si la réponse était un flux {@code text/event-stream}
 */
public record McpResponseInfo(
        Integer statusCode,
        String contentType,
        String sessionId,
        long byteCount,
        long chunkCount,
        long durationMillis,
        boolean sse) {
}
