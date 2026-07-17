package com.example.demo_gateway_ai.mcp;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(McpListenerProperties.class)
public class McpListenerConfiguration {
}
