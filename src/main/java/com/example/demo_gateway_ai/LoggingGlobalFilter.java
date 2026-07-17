package com.example.demo_gateway_ai;

import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.filter.NettyWriteResponseFilter;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
public class LoggingGlobalFilter implements GlobalFilter, Ordered {

    private static final int MAX_BODY_LOG_CHARS = 4096;

    private static final Set<String> LOGGABLE_CONTENT_TYPE_PREFIXES = Set.of(
            "text/",
            "application/json",
            "application/xml",
            "application/x-ndjson",
            "application/x-www-form-urlencoded"
    );

    @Override
    public int getOrder() {
        return NettyWriteResponseFilter.WRITE_RESPONSE_FILTER_ORDER - 1;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();

        if (log.isDebugEnabled()) {
            log.debug("REQUEST: {} {}\nHEADERS:\n{}",
                    request.getMethod(),
                    request.getURI(),
                    formatHeaders(request.getHeaders()));
        }

        return chain.filter(
                exchange.mutate()
                        .request(new LoggingRequestDecorator(request))
                        .response(new LoggingResponseDecorator(exchange.getResponse()))
                        .build()
        );
    }

    private static final class LoggingRequestDecorator extends ServerHttpRequestDecorator {

        LoggingRequestDecorator(ServerHttpRequest delegate) {
            super(delegate);
        }

        @Override
        public Flux<DataBuffer> getBody() {
            return super.getBody().doOnNext(buffer -> {
                if (log.isDebugEnabled()) {
                    logChunk("REQUEST  BODY", getHeaders().getContentType(), buffer);
                }
            });
        }
    }

    private static final class LoggingResponseDecorator extends ServerHttpResponseDecorator {

        private final AtomicBoolean headersLogged = new AtomicBoolean(false);

        LoggingResponseDecorator(ServerHttpResponse delegate) {
            super(delegate);
        }

        @Override
        public Mono<Void> writeWith(Publisher<? extends DataBuffer> body) {
            logResponseHeadersOnce();
            return super.writeWith(
                    Flux.from(body).doOnNext(buffer -> {
                        if (log.isDebugEnabled()) {
                            logChunk("RESPONSE BODY", getHeaders().getContentType(), buffer);
                        }
                    })
            );
        }

        @Override
        public Mono<Void> writeAndFlushWith(Publisher<? extends Publisher<? extends DataBuffer>> body) {
            logResponseHeadersOnce();
            return super.writeAndFlushWith(
                    Flux.from(body).map(inner ->
                            Flux.from(inner).doOnNext(buffer -> {
                                if (log.isDebugEnabled()) {
                                    logChunk("RESPONSE BODY", getHeaders().getContentType(), buffer);
                                }
                            })
                    )
            );
        }

        private void logResponseHeadersOnce() {
            if (log.isDebugEnabled() && headersLogged.compareAndSet(false, true)) {
                log.debug("RESPONSE STATUS : {}, HEADERS:\n{}", getStatusCode(), formatHeaders(getHeaders()));
            }
        }
    }

    private static void logChunk(String label, MediaType contentType, DataBuffer buffer) {
        int byteCount = buffer.readableByteCount();
        if (!isLoggableContentType(contentType)) {
            log.debug("{} [binary, {} bytes, content-type: {}]", label, byteCount, contentType);
            return;
        }
        Charset charset = contentType != null && contentType.getCharset() != null
                ? contentType.getCharset()
                : StandardCharsets.UTF_8;
        // toString(Charset) reads positionally without advancing the buffer's read index
        String body = buffer.toString(charset);
        if (body.length() > MAX_BODY_LOG_CHARS) {
            log.debug("{} [{} bytes, truncated]: {}...", label, byteCount, body.substring(0, MAX_BODY_LOG_CHARS));
        } else {
            log.debug("{} [{} bytes]: {}", label, byteCount, body);
        }
    }

    private static boolean isLoggableContentType(MediaType contentType) {
        if (contentType == null) return false;
        String type = contentType.getType() + "/" + contentType.getSubtype();
        return LOGGABLE_CONTENT_TYPE_PREFIXES.stream().anyMatch(type::startsWith);
    }

    private static String formatHeaders(HttpHeaders headers) {
        StringBuilder sb = new StringBuilder();
        headers.forEach((name, values) ->
                values.forEach(v -> sb.append("  ").append(name).append(": ").append(v).append("\n"))
        );
        return sb.toString();
    }
}
