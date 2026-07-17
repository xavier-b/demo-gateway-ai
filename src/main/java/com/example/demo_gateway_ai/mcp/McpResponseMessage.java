package com.example.demo_gateway_ai.mcp;

/**
 * Message JSON-RPC de réponse extrait du flux SSE au fil de l'eau (EF-6).
 *
 * @param id           identifiant JSON-RPC de corrélation ({@code null} si absent)
 * @param hasResult    présence d'un champ {@code $.result}
 * @param hasError     présence d'un champ {@code $.error} non nul
 * @param errorCode    {@code $.error.code} si présent
 * @param errorMessage {@code $.error.message} si présent
 */
public record McpResponseMessage(
        String id,
        boolean hasResult,
        boolean hasError,
        Integer errorCode,
        String errorMessage) {
}
