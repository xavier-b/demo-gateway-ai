package com.example.demo_gateway_ai.mcp;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration du composant d'écoute MCP (préfixe {@code mcp.listener}, cf. ENF-5).
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "mcp.listener")
public class McpListenerProperties {

    /** Active ou désactive complètement le filtre. */
    private boolean enabled = true;

    /** Identifiants de routes Spring Cloud Gateway considérées comme MCP (ex. {@code mcp-weather}). */
    private List<String> routeIds = new ArrayList<>();

    /** Patterns de chemin considérés comme MCP (ex. {@code /mcp*}, {@code /mcp/**}). */
    private List<String> pathPatterns = new ArrayList<>();

    /** Taille maximale du corps de requête bufferisé ; au-delà la gateway répond 413 (EF-2). */
    private DataSize maxRequestBytes = DataSize.ofKilobytes(256);

    /** Taille maximale accumulée par évènement SSE en cours de parsing ; au-delà l'évènement est abandonné (EF-6). */
    private DataSize maxSseEventBytes = DataSize.ofKilobytes(16);

    /** Loggue les payloads (tronqués) des requêtes MCP. */
    private boolean logPayloads = true;

    /** Préfixe des clés d'attributs d'exchange publiés (EF-7). */
    private String attributePrefix = "mcp.";
}
