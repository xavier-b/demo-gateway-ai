package com.example.demo_gateway_ai;

import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GatewayConfig {

    @Bean
    public RouteLocator ollamaRoute(RouteLocatorBuilder builder) {
        return builder.routes()
                .route("ollama-route", r -> r
                        .path("/ollama/**")
                        .filters(f -> f.stripPrefix(1))
                        .uri("http://localhost:11434"))
                .route("mcp-weather", r -> r
                        .path("/mcp-weather/**")
                        .filters(f -> f.stripPrefix(1))
                        .uri("http://localhost:8080"))

                .build();
    }
}
