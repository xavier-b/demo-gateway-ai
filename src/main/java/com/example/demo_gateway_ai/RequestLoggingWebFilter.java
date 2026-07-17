package com.example.demo_gateway_ai;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Set;

@Slf4j
//@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RequestLoggingWebFilter implements WebFilter {

    private static final int MAX_BODY_LOG_CHARS = 4096;

    private static final Set<String> LOGGABLE_CONTENT_TYPE_PREFIXES = Set.of(
            "text/",
            "application/json",
            "application/xml",
            "application/x-ndjson",
            "application/x-www-form-urlencoded"
    );

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();

        if (log.isDebugEnabled()) {
            log.debug("WEB REQUEST: {} {}\nHEADERS:\n{}",
                    request.getMethod(),
                    request.getURI(),
                    formatHeaders(request.getHeaders()));
        }

        return DataBufferUtils.join(request.getBody())
                .flatMap(dataBuffer -> {
                    byte[] bytes = new byte[dataBuffer.readableByteCount()];
                    dataBuffer.read(bytes);
                    DataBufferUtils.release(dataBuffer);

                    if (log.isDebugEnabled()) {
                        logBody(request.getHeaders().getContentType(), bytes);
                    }

                    DataBuffer rewrapped = exchange.getResponse().bufferFactory().wrap(bytes);
                    ServerHttpRequest mutated = new ServerHttpRequestDecorator(request) {
                        @Override
                        public Flux<DataBuffer> getBody() {
                            return Flux.just(rewrapped);
                        }
                    };
                    return chain.filter(exchange.mutate().request(mutated).build());
                })
                .switchIfEmpty(chain.filter(exchange));
    }

    private static void logBody(MediaType contentType, byte[] bytes) {
        int byteCount = bytes.length;
        if (!isLoggableContentType(contentType)) {
            log.debug("WEB REQUEST BODY [binary, {} bytes, content-type: {}]", byteCount, contentType);
            return;
        }
        Charset charset = contentType != null && contentType.getCharset() != null
                ? contentType.getCharset()
                : StandardCharsets.UTF_8;
        String body = new String(bytes, charset);
        if (body.length() > MAX_BODY_LOG_CHARS) {
            log.debug("WEB REQUEST BODY [{} bytes, truncated]: {}...", byteCount, body.substring(0, MAX_BODY_LOG_CHARS));
        } else {
            log.debug("WEB REQUEST BODY [{} bytes]: {}", byteCount, body);
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
