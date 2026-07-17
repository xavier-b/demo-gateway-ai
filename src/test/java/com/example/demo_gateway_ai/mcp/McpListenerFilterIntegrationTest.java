package com.example.demo_gateway_ai.mcp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.Ordered;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.FluxExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.util.function.Tuple2;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cas de test de la spec §6 : gateway réelle + backend MCP simulé (reactor-netty).
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "mcp.listener.route-ids=mcp-test",
                "mcp.listener.max-request-bytes=2KB",
                "mcp.listener.max-sse-event-bytes=16KB"
        })
@Import(McpListenerFilterIntegrationTest.TestSupportConfig.class)
class McpListenerFilterIntegrationTest {

    private static final String SSE_EVENT_1 = "data: {\"jsonrpc\":\"2.0\",\"id\":\"1\",\"result\":{\"v\":1}}\n\n";
    private static final String SSE_EVENT_3 =
            "data: {\"jsonrpc\":\"2.0\",\"id\":\"3\",\"error\":{\"code\":-32601,\"message\":\"not found\"}}\n\n";

    private static final List<byte[]> backendBodies = new CopyOnWriteArrayList<>();
    private static final List<String> backendPaths = new CopyOnWriteArrayList<>();

    private static final DisposableServer backend = HttpServer.create()
            .port(0)
            .route(routes -> routes
                    .post("/rpc", (req, res) ->
                            req.receive().aggregate().asByteArray().defaultIfEmpty(new byte[0])
                                    .flatMap(bytes -> {
                                        backendBodies.add(bytes);
                                        backendPaths.add(req.fullPath());
                                        return res.status(200)
                                                .header("Content-Type", "application/json")
                                                .header("Mcp-Session-Id", "backend-session-1")
                                                .sendString(Mono.just(
                                                        "{\"jsonrpc\":\"2.0\",\"id\":\"1\",\"result\":{\"ok\":true}}"))
                                                .then();
                                    }))
                    .get("/sse", (req, res) -> {
                        backendPaths.add(req.fullPath());
                        // évènement 2 volontairement coupé au milieu d'une ligne data: et du JSON (5bis)
                        Flux<ByteBuf> chunks = Flux.concat(
                                Mono.just(buf(SSE_EVENT_1)),
                                delayed(150, "data: {\"jsonrpc\":\"2.0\",\"id\":\"2\",\"re"),
                                delayed(50, "sult\":{\"content\":[{\"type\":\"te"),
                                delayed(50, "xt\"}]}}\n\n"),
                                delayed(150, SSE_EVENT_3));
                        return res.status(200)
                                .header("Content-Type", "text/event-stream")
                                .send(chunks, b -> true)
                                .then();
                    })
                    .get("/sse-big", (req, res) -> {
                        String oversized = "data: {\"jsonrpc\":\"2.0\",\"id\":\"big\",\"result\":{\"blob\":\""
                                + "x".repeat(20_000) + "\"}}\n\n";
                        String after = "data: {\"jsonrpc\":\"2.0\",\"id\":\"after\",\"result\":{}}\n\n";
                        return res.status(200)
                                .header("Content-Type", "text/event-stream")
                                .send(Flux.just(buf(oversized), buf(after)), b -> true)
                                .then();
                    })
                    .delete("/session", (req, res) -> {
                        backendPaths.add(req.fullPath());
                        return res.status(204).send();
                    }))
            .bindNow();

    @DynamicPropertySource
    static void backendUri(DynamicPropertyRegistry registry) {
        registry.add("test.backend.uri", () -> "http://localhost:" + backend.port());
    }

    @AfterAll
    static void stopBackend() {
        backend.disposeNow();
    }

    private static ByteBuf buf(String s) {
        return Unpooled.wrappedBuffer(s.getBytes(StandardCharsets.UTF_8));
    }

    private static Mono<ByteBuf> delayed(long millis, String s) {
        return Mono.delay(Duration.ofMillis(millis)).thenReturn(buf(s));
    }

    @Value("${local.server.port}")
    private int gatewayPort;

    private WebTestClient client;

    @Autowired
    private CapturingListener listener;

    @BeforeEach
    void resetCaptures() {
        client = WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + gatewayPort)
                .responseTimeout(Duration.ofSeconds(15))
                .build();
        backendBodies.clear();
        backendPaths.clear();
        listener.clear();
        AttributeCapturingFilter.ATTRS.clear();
    }

    // ------------------------------------------------------------------ cas 1 et 6

    @Test
    void toolsCall_publishesInfoAndProxiesPayloadByteForByte() {
        byte[] payload = ("{\"jsonrpc\":\"2.0\",\"id\":42,\"method\":\"tools/call\","
                + "\"params\":{\"name\":\"get_weather\",\"arguments\":{\"city\":\"Méribel ☔\"}}}")
                .getBytes(StandardCharsets.UTF_8);

        client.post().uri("/mcp-test/rpc")
                .contentType(MediaType.APPLICATION_JSON)
                .header("Mcp-Session-Id", "sess-42")
                .bodyValue(payload)
                .exchange()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.result.ok").isEqualTo(true);

        assertEquals(1, backendBodies.size());
        assertArrayEquals(payload, backendBodies.get(0), "le backend doit recevoir le payload intact");

        assertEquals(1, listener.requests.size());
        McpRequestInfo info = listener.requests.get(0);
        assertEquals("tools/call", info.method());
        assertEquals("get_weather", info.toolName());
        assertEquals("42", info.id());
        assertEquals("sess-42", info.sessionId());

        Map<String, Object> attrs = AttributeCapturingFilter.ATTRS.get("/mcp-test/rpc");
        assertEquals("tools/call", attrs.get("mcp.method"));
        assertEquals("get_weather", attrs.get("mcp.toolName"));
        assertEquals(false, attrs.get("mcp.isBatch"));

        // cas 6 : réponse application/json simple → métriques correctes à la terminaison
        waitUntil(() -> !listener.completes.isEmpty());
        McpResponseInfo response = listener.completes.get(0);
        assertEquals(200, response.statusCode());
        assertTrue(response.contentType().startsWith("application/json"));
        assertEquals("backend-session-1", response.sessionId());
        assertTrue(response.byteCount() > 0);
        assertFalse(response.sse());
    }

    // ------------------------------------------------------------------ cas 2

    @Test
    void notificationBatchAndInitialize_areExtracted() {
        postJson("{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}");
        McpRequestInfo notification = listener.requests.get(listener.requests.size() - 1);
        assertNull(notification.id());
        assertEquals("notifications/initialized", notification.method());

        postJson("[{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"},"
                + "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}]");
        McpRequestInfo batch = listener.requests.get(listener.requests.size() - 1);
        assertTrue(batch.batch());
        assertEquals(List.of("tools/list", "notifications/initialized"), batch.methods());
        assertEquals(true, AttributeCapturingFilter.ATTRS.get("/mcp-test/rpc").get("mcp.isBatch"));

        postJson("{\"jsonrpc\":\"2.0\",\"id\":0,\"method\":\"initialize\","
                + "\"params\":{\"protocolVersion\":\"2025-03-26\"}}");
        McpRequestInfo initialize = listener.requests.get(listener.requests.size() - 1);
        assertEquals("2025-03-26", initialize.protocolVersion());
    }

    // ------------------------------------------------------------------ cas 3

    @Test
    void invalidJson_isProxiedUntouchedWithoutHttpError() {
        byte[] payload = "{this is not json at all".getBytes(StandardCharsets.UTF_8);

        client.post().uri("/mcp-test/rpc")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(payload)
                .exchange()
                .expectStatus().isOk();

        assertEquals(1, backendBodies.size());
        assertArrayEquals(payload, backendBodies.get(0));
        assertFalse(listener.requests.get(0).parsed());
    }

    // ------------------------------------------------------------------ cas 4

    @Test
    void oversizedRequest_isRejectedWith413JsonRpcError() {
        String big = "{\"jsonrpc\":\"2.0\",\"method\":\"tools/call\",\"params\":{\"blob\":\""
                + "x".repeat(3000) + "\"}}";

        client.post().uri("/mcp-test/rpc")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(big)
                .exchange()
                .expectStatus().isEqualTo(413)
                .expectBody()
                .jsonPath("$.jsonrpc").isEqualTo("2.0")
                .jsonPath("$.error.code").isEqualTo(-32600)
                .jsonPath("$.error.message").isEqualTo("Request payload too large");

        assertTrue(backendBodies.isEmpty(), "la requête ne doit pas être transmise au backend");
    }

    // ------------------------------------------------------------------ cas 5, 5bis et 8 (GET SSE)

    @Test
    void sseResponse_isStreamedIncrementallyAndMessagesExtracted() {
        FluxExchangeResult<String> result = client.get().uri("/mcp-test/sse")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .header("Mcp-Session-Id", "sess-sse")
                .exchange()
                .expectStatus().isOk()
                .returnResult(String.class);

        List<Tuple2<Long, String>> events = result.getResponseBody()
                .timestamp()
                .collectList()
                .block(Duration.ofSeconds(10));

        assertEquals(3, events.size());
        assertTrue(events.get(0).getT2().contains("\"id\":\"1\""));
        assertTrue(events.get(2).getT2().contains("-32601"));

        // non-buffering : si la gateway agrégeait, les 3 évènements arriveraient simultanément
        long firstToLast = events.get(2).getT1() - events.get(0).getT1();
        assertTrue(firstToLast >= 200,
                "les évènements doivent arriver au fil de l'eau (écart mesuré: " + firstToLast + " ms)");

        // 5bis : l'évènement 2, coupé en plein milieu d'une ligne data: et du JSON, est extrait
        waitUntil(() -> listener.responseMessages.size() == 3);
        assertEquals("1", listener.responseMessages.get(0).id());
        McpResponseMessage split = listener.responseMessages.get(1);
        assertEquals("2", split.id());
        assertTrue(split.hasResult());
        McpResponseMessage error = listener.responseMessages.get(2);
        assertEquals("3", error.id());
        assertTrue(error.hasError());
        assertEquals(-32601, error.errorCode());
        assertEquals("not found", error.errorMessage());

        // cas 8 : Mcp-Session-Id de la requête GET capturé sans bufferisation
        McpRequestInfo request = listener.requests.get(0);
        assertNull(request.method());
        assertEquals("sess-sse", request.sessionId());

        waitUntil(() -> !listener.completes.isEmpty());
        assertTrue(listener.completes.get(0).sse());
    }

    @Test
    void oversizedSseEvent_isAbandonedWithoutDisturbingTheStream() {
        FluxExchangeResult<String> result = client.get().uri("/mcp-test/sse-big")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .returnResult(String.class);

        List<String> events = result.getResponseBody().collectList().block(Duration.ofSeconds(10));

        // le client reçoit les deux évènements intacts, y compris celui dépassant la limite
        assertEquals(2, events.size());
        assertEquals(20_000 + "{\"jsonrpc\":\"2.0\",\"id\":\"big\",\"result\":{\"blob\":\"\"}}".length(),
                events.get(0).length());
        assertTrue(events.get(1).contains("\"id\":\"after\""));

        // seul l'évènement sous la limite est extrait
        waitUntil(() -> !listener.responseMessages.isEmpty());
        assertEquals(1, listener.responseMessages.size());
        assertEquals("after", listener.responseMessages.get(0).id());
    }

    // ------------------------------------------------------------------ cas 8 (DELETE)

    @Test
    void deleteSession_capturesSessionIdWithoutBuffering() {
        client.delete().uri("/mcp-test/session")
                .header("Mcp-Session-Id", "sess-to-close")
                .exchange()
                .expectStatus().isNoContent();

        assertEquals(1, listener.requests.size());
        McpRequestInfo info = listener.requests.get(0);
        assertNull(info.method());
        assertEquals("sess-to-close", info.sessionId());
        assertEquals("sess-to-close",
                AttributeCapturingFilter.ATTRS.get("/mcp-test/session").get("mcp.sessionId"));
    }

    // ------------------------------------------------------------------ cas 7

    @Test
    void nonMcpTraffic_isNotDecoratedNorObserved() {
        client.post().uri("/plain/rpc")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"x\"}}")
                .exchange()
                .expectStatus().isOk();

        assertTrue(listener.requests.isEmpty(), "aucun listener ne doit être notifié hors routes MCP");
        assertFalse(AttributeCapturingFilter.ATTRS.containsKey("/plain/rpc"),
                "aucun attribut mcp.* ne doit être publié hors routes MCP");
    }

    /** Attente courte (max 2 s) d'une condition asynchrone (callbacks de terminaison). */
    private static void waitUntil(java.util.function.BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + 2000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("condition non remplie après 2 s");
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }

    private void postJson(String body) {
        client.post().uri("/mcp-test/rpc")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .exchange()
                .expectStatus().isOk();
    }

    // ------------------------------------------------------------------ infrastructure de test

    @TestConfiguration(proxyBeanMethods = false)
    static class TestSupportConfig {

        @Bean
        RouteLocator testRoutes(RouteLocatorBuilder builder, @Value("${test.backend.uri}") String backendUri) {
            return builder.routes()
                    .route("mcp-test", r -> r.path("/mcp-test/**")
                            .filters(f -> f.stripPrefix(1))
                            .uri(backendUri))
                    .route("plain-test", r -> r.path("/plain/**")
                            .filters(f -> f.stripPrefix(1))
                            .uri(backendUri))
                    .build();
        }

        @Bean
        CapturingListener capturingListener() {
            return new CapturingListener();
        }

        @Bean
        AttributeCapturingFilter attributeCapturingFilter() {
            return new AttributeCapturingFilter();
        }
    }

    /** Vérifie le point d'extension EF-7. */
    static class CapturingListener implements McpEventListener {
        final List<McpRequestInfo> requests = new CopyOnWriteArrayList<>();
        final List<McpResponseMessage> responseMessages = new CopyOnWriteArrayList<>();
        final List<McpResponseInfo> completes = new CopyOnWriteArrayList<>();

        @Override
        public void onRequest(ServerWebExchange exchange, McpRequestInfo info) {
            requests.add(info);
        }

        @Override
        public void onResponseMessage(ServerWebExchange exchange, McpResponseMessage message) {
            responseMessages.add(message);
        }

        @Override
        public void onComplete(ServerWebExchange exchange, McpResponseInfo info) {
            completes.add(info);
        }

        void clear() {
            requests.clear();
            responseMessages.clear();
            completes.clear();
        }
    }

    /** Filtre aval (ordre 0 &gt; ordre du McpListenerFilter) : vérifie les attributs publiés (EF-7). */
    static class AttributeCapturingFilter implements GlobalFilter, Ordered {
        static final Map<String, Map<String, Object>> ATTRS = new ConcurrentHashMap<>();

        @Override
        public int getOrder() {
            return 0;
        }

        @Override
        public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
            Map<String, Object> mcpAttrs = new HashMap<>();
            exchange.getAttributes().forEach((key, value) -> {
                if (key.startsWith("mcp.")) {
                    mcpAttrs.put(key, value);
                }
            });
            if (!mcpAttrs.isEmpty()) {
                ATTRS.put(exchange.getRequest().getPath().value(), mcpAttrs);
            }
            return chain.filter(exchange);
        }
    }
}
