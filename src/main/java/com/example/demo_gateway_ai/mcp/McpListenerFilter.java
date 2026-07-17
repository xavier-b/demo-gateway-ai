package com.example.demo_gateway_ai.mcp;

import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.filter.NettyWriteResponseFilter;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferLimitException;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.util.pattern.PathPattern;
import org.springframework.web.util.pattern.PathPatternParser;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Composant d'écoute des requêtes MCP (cf. spec-listner-mcp.md).
 *
 * <p>Asymétrie au cœur de la conception : <b>join borné côté requête</b> (payload petit, fini,
 * nécessaire en entier pour le parsing JSON-RPC, rejet 413 au-delà de la limite — EF-2/EF-3)
 * vs <b>observation passive chunk par chunk côté réponse</b> (flux potentiellement infini,
 * jamais agrégé, jamais interrompu — EF-5/EF-6). Le filtre est strictement en lecture seule :
 * les octets proxifiés sont identiques à l'original (ENF-3).
 */
@Slf4j
@Component
public class McpListenerFilter implements GlobalFilter, Ordered {

    static final String MCP_SESSION_ID_HEADER = "Mcp-Session-Id";

    private static final int MAX_PAYLOAD_LOG_CHARS = 4096;
    private static final byte[] EMPTY_BODY = new byte[0];
    private static final String PAYLOAD_TOO_LARGE_BODY =
            "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32600,\"message\":\"Request payload too large\"},\"id\":null}";

    private final McpListenerProperties properties;
    private final List<McpEventListener> listeners;
    private final List<PathPattern> pathPatterns;

    public McpListenerFilter(McpListenerProperties properties, ObjectProvider<McpEventListener> listeners) {
        this.properties = properties;
        this.listeners = listeners.orderedStream().toList();
        this.pathPatterns = properties.getPathPatterns().stream()
                .map(PathPatternParser.defaultInstance::parse)
                .toList();
    }

    @Override
    public int getOrder() {
        // ENF-4 : décore la réponse avant son écriture Netty, après la résolution de route
        return NettyWriteResponseFilter.WRITE_RESPONSE_FILTER_ORDER - 1;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        if (!properties.isEnabled() || !matches(exchange)) {
            return chain.filter(exchange);
        }

        ServerHttpRequest request = exchange.getRequest();
        String sessionId = request.getHeaders().getFirst(MCP_SESSION_ID_HEADER);
        McpResponseDecorator decoratedResponse = new McpResponseDecorator(exchange);

        if (request.getMethod() == HttpMethod.POST && isJsonCompatible(request.getHeaders().getContentType())) {
            int maxRequestBytes = (int) properties.getMaxRequestBytes().toBytes();
            return DataBufferUtils.join(request.getBody(), maxRequestBytes)
                    .map(McpListenerFilter::consumeToByteArray)
                    .defaultIfEmpty(EMPTY_BODY)
                    // seul join() peut lever DataBufferLimitException ici : les buffers déjà
                    // reçus sont libérés par join(), la requête n'est pas transmise au backend
                    .onErrorResume(DataBufferLimitException.class,
                            e -> rejectPayloadTooLarge(exchange).then(Mono.empty()))
                    .flatMap(bytes -> {
                        McpRequestInfo info = JsonRpcRequestParser.parse(bytes, sessionId);
                        publishRequestInfo(exchange, info, bytes);
                        ServerHttpRequest replayedRequest =
                                bytes.length == 0 ? request : new ReplayingRequestDecorator(exchange, request, bytes);
                        return chain.filter(exchange.mutate()
                                .request(replayedRequest)
                                .response(decoratedResponse)
                                .build());
                    });
        }

        // GET (ouverture de flux SSE), DELETE (fin de session), etc. : headers/URI seulement (EF-2)
        publishRequestInfo(exchange, McpRequestInfo.headersOnly(sessionId), null);
        return chain.filter(exchange.mutate().response(decoratedResponse).build());
    }

    // ---------------------------------------------------------------- EF-1 : ciblage

    private boolean matches(ServerWebExchange exchange) {
        Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
        if (route != null && properties.getRouteIds().contains(route.getId())) {
            return true;
        }
        if (!pathPatterns.isEmpty()) {
            var path = exchange.getRequest().getPath().pathWithinApplication();
            for (PathPattern pattern : pathPatterns) {
                if (pattern.matches(path)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isJsonCompatible(MediaType contentType) {
        return contentType != null && MediaType.APPLICATION_JSON.isCompatibleWith(contentType);
    }

    // ---------------------------------------------------------------- EF-2 : rejet 413

    private Mono<Void> rejectPayloadTooLarge(ServerWebExchange exchange) {
        Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
        log.warn("Requête MCP rejetée en 413: Content-Length={}, limite={} octets, route={}",
                exchange.getRequest().getHeaders().getFirst("Content-Length"),
                properties.getMaxRequestBytes().toBytes(),
                route != null ? route.getId() : "?");
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(HttpStatus.PAYLOAD_TOO_LARGE);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        byte[] body = PAYLOAD_TOO_LARGE_BODY.getBytes(StandardCharsets.UTF_8);
        return response.writeWith(Mono.fromSupplier(() -> response.bufferFactory().wrap(body)));
    }

    private static byte[] consumeToByteArray(DataBuffer buffer) {
        byte[] bytes = new byte[buffer.readableByteCount()];
        buffer.read(bytes);
        DataBufferUtils.release(buffer);
        return bytes;
    }

    // ---------------------------------------------------------------- EF-7 : publication

    private void publishRequestInfo(ServerWebExchange exchange, McpRequestInfo info, byte[] payload) {
        String prefix = properties.getAttributePrefix();
        putIfNotNull(exchange, prefix + "method", info.method());
        putIfNotNull(exchange, prefix + "id", info.id());
        putIfNotNull(exchange, prefix + "toolName", info.toolName());
        putIfNotNull(exchange, prefix + "protocolVersion", info.protocolVersion());
        putIfNotNull(exchange, prefix + "sessionId", info.sessionId());
        exchange.getAttributes().put(prefix + "isBatch", info.batch());
        if (!info.methods().isEmpty()) {
            exchange.getAttributes().put(prefix + "methods", info.methods());
        }
        exchange.getAttributes().put(prefix + "requestInfo", info);

        if (!info.parsed() && payload != null && payload.length > 0) {
            log.warn("Payload MCP non interprétable comme JSON-RPC (pass-through intact): {}",
                    truncate(payloadAsString(exchange, payload)));
        } else if ("tools/call".equals(info.method())) {
            log.info("MCP {} tool={} id={} sessionId={}",
                    info.method(), info.toolName(), info.id(), info.sessionId());
        } else if (log.isDebugEnabled()) {
            log.debug("MCP method={} id={} batch={} sessionId={}{}",
                    info.method(), info.id(), info.batch(), info.sessionId(),
                    properties.isLogPayloads() && payload != null && payload.length > 0
                            ? " payload=" + truncate(payloadAsString(exchange, payload))
                            : "");
        }
        notifyListeners(listener -> listener.onRequest(exchange, info));
    }

    private static void putIfNotNull(ServerWebExchange exchange, String key, String value) {
        if (value != null) {
            exchange.getAttributes().put(key, value);
        }
    }

    private void notifyListeners(Consumer<McpEventListener> callback) {
        for (McpEventListener listener : listeners) {
            try {
                callback.accept(listener);
            } catch (Exception e) {
                log.warn("McpEventListener {} a levé une exception (ignorée)", listener.getClass().getName(), e);
            }
        }
    }

    private static String payloadAsString(ServerWebExchange exchange, byte[] payload) {
        MediaType contentType = exchange.getRequest().getHeaders().getContentType();
        Charset charset = contentType != null && contentType.getCharset() != null
                ? contentType.getCharset()
                : StandardCharsets.UTF_8;
        return new String(payload, charset);
    }

    private static String truncate(String value) {
        return value.length() > MAX_PAYLOAD_LOG_CHARS
                ? value.substring(0, MAX_PAYLOAD_LOG_CHARS) + "..."
                : value;
    }

    // ---------------------------------------------------------------- EF-2 : ré-émission requête

    /** Ré-expose à l'identique les octets bufferisés vers la chaîne de filtres (proxying intact). */
    private static final class ReplayingRequestDecorator extends ServerHttpRequestDecorator {

        private final ServerWebExchange exchange;
        private final byte[] bytes;

        ReplayingRequestDecorator(ServerWebExchange exchange, ServerHttpRequest delegate, byte[] bytes) {
            super(delegate);
            this.exchange = exchange;
            this.bytes = bytes;
        }

        @Override
        public Flux<DataBuffer> getBody() {
            // defer : chaque souscription reçoit un buffer neuf enveloppant les mêmes octets
            return Flux.defer(() -> Flux.just(exchange.getResponse().bufferFactory().wrap(bytes)));
        }
    }

    // ---------------------------------------------------------------- EF-5 / EF-6 : réponse

    /**
     * Observation passive et non bufferisante du flux de réponse : les {@code DataBuffer} sont
     * inspectés positionnellement puis transmis tels quels ; {@code writeWith} et
     * {@code writeAndFlushWith} (indispensable pour le SSE) sont tous deux décorés ; la
     * contre-pression est préservée (aucun opérateur d'agrégation ni de délai).
     */
    private final class McpResponseDecorator extends ServerHttpResponseDecorator {

        private final ServerWebExchange exchange;
        private final long startNanos = System.nanoTime();
        private final AtomicBoolean firstWrite = new AtomicBoolean();
        private final AtomicBoolean terminated = new AtomicBoolean();
        private final AtomicLong byteCount = new AtomicLong();
        private final AtomicLong chunkCount = new AtomicLong();

        private volatile SseEventAssembler sseAssembler;
        private volatile boolean observerBroken;

        McpResponseDecorator(ServerWebExchange exchange) {
            super(exchange.getResponse());
            this.exchange = exchange;
        }

        @Override
        public Mono<Void> writeWith(Publisher<? extends DataBuffer> body) {
            onFirstWrite();
            return super.writeWith(Flux.from(body).doOnNext(this::observe))
                    .doFinally(this::onTerminate);
        }

        @Override
        public Mono<Void> writeAndFlushWith(Publisher<? extends Publisher<? extends DataBuffer>> body) {
            onFirstWrite();
            return super.writeAndFlushWith(
                    Flux.from(body).map(inner -> Flux.from(inner).doOnNext(this::observe)))
                    .doFinally(this::onTerminate);
        }

        private void onFirstWrite() {
            if (!firstWrite.compareAndSet(false, true)) {
                return;
            }
            MediaType contentType = getHeaders().getContentType();
            String responseSessionId = getHeaders().getFirst(MCP_SESSION_ID_HEADER);
            if (responseSessionId != null) {
                exchange.getAttributes().put(properties.getAttributePrefix() + "responseSessionId", responseSessionId);
            }
            log.debug("MCP réponse: status={} contentType={} sessionId={}",
                    getStatusCode(), contentType, responseSessionId);
            if (contentType != null && MediaType.TEXT_EVENT_STREAM.isCompatibleWith(contentType)) {
                sseAssembler = new SseEventAssembler(
                        properties.getMaxSseEventBytes().toBytes(),
                        message -> {
                            log.debug("MCP message de réponse SSE: {}", message);
                            notifyListeners(listener -> listener.onResponseMessage(exchange, message));
                        });
            }
        }

        /** Ne doit jamais lever : une erreur d'observation ne doit pas perturber le proxying. */
        private void observe(DataBuffer buffer) {
            try {
                int readable = buffer.readableByteCount();
                byteCount.addAndGet(readable);
                chunkCount.incrementAndGet();
                SseEventAssembler assembler = sseAssembler;
                if (assembler != null && readable > 0 && !observerBroken) {
                    byte[] copy = copyReadableBytes(buffer, readable);
                    assembler.onChunk(copy, 0, copy.length);
                }
                notifyListeners(listener -> listener.onResponseChunk(exchange, readable));
            } catch (Exception e) {
                observerBroken = true;
                log.warn("Observation de la réponse MCP désactivée pour ce flux", e);
            }
        }

        private void onTerminate(SignalType signal) {
            if (!terminated.compareAndSet(false, true)) {
                return;
            }
            SseEventAssembler assembler = sseAssembler;
            if (assembler != null) {
                assembler.close();
            }
            MediaType contentType = getHeaders().getContentType();
            McpResponseInfo info = new McpResponseInfo(
                    getStatusCode() != null ? getStatusCode().value() : null,
                    contentType != null ? contentType.toString() : null,
                    getHeaders().getFirst(MCP_SESSION_ID_HEADER),
                    byteCount.get(),
                    chunkCount.get(),
                    Duration.ofNanos(System.nanoTime() - startNanos).toMillis(),
                    assembler != null);
            log.debug("MCP réponse terminée ({}): {}", signal, info);
            notifyListeners(listener -> listener.onComplete(exchange, info));
        }
    }

    /** Copie positionnelle des octets lisibles, sans avancer l'index de lecture du buffer. */
    private static byte[] copyReadableBytes(DataBuffer buffer, int readable) {
        byte[] copy = new byte[readable];
        int position = 0;
        try (DataBuffer.ByteBufferIterator iterator = buffer.readableByteBuffers()) {
            while (iterator.hasNext() && position < readable) {
                ByteBuffer byteBuffer = iterator.next();
                int n = Math.min(byteBuffer.remaining(), readable - position);
                byteBuffer.get(copy, position, n);
                position += n;
            }
        }
        return copy;
    }
}
