package com.example.demo_gateway_ai.mcp;

import org.springframework.web.server.ServerWebExchange;

/**
 * Point d'extension du composant d'écoute MCP (EF-7) : tout bean implémentant cette
 * interface est notifié des évènements observés, ce qui permet de découpler l'analyse
 * (audit, métriques Micrometer, …) du filtre lui-même.
 *
 * <p>Les callbacks sont invoqués sur le pipeline réactif : ils doivent être non bloquants
 * et rapides (ENF-1). Toute exception levée est interceptée et loguée, jamais propagée.
 */
public interface McpEventListener {

    /** Invoqué avant l'appel au backend, une fois les informations de requête extraites. */
    default void onRequest(ServerWebExchange exchange, McpRequestInfo info) {
    }

    /** Invoqué pour chaque chunk de réponse écrit vers le client (observation passive). */
    default void onResponseChunk(ServerWebExchange exchange, int byteCount) {
    }

    /** Invoqué pour chaque message JSON-RPC extrait du flux de réponse SSE (EF-6). */
    default void onResponseMessage(ServerWebExchange exchange, McpResponseMessage message) {
    }

    /** Invoqué à la terminaison (complétion, erreur ou annulation) du flux de réponse. */
    default void onComplete(ServerWebExchange exchange, McpResponseInfo info) {
    }
}
