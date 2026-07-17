# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**demo-gateway-ai** is a Spring Cloud Gateway application — a reactive API gateway built on Spring WebFlux. It acts as an entry point for routing and filtering HTTP traffic to backend services.

- **Java 21**, **Spring Boot 4.0.6**, **Spring Cloud 2025.1.1**
- Reactive stack (Project Reactor / WebFlux), not a traditional servlet-based app
- Build system: **Maven**

## Common Commands

```bash
# Run the application
mvn spring-boot:run

# Build (skip tests)
mvn clean package -DskipTests

# Run all tests
mvn test

# Run a single test class
mvn test -Dtest=DemoGatewayAiApplicationTests

# Build a runnable Docker/OCI image
mvn spring-boot:build-image
```

## Architecture

The app is a standard Spring Boot single-module Maven project. The main package is `com.example.demo_gateway_ai` (note underscore — the original hyphenated name was invalid as a Java package).

**Entry point:** `src/main/java/com/example/demo_gateway_ai/DemoGatewayAiApplication.java`

**Configuration:** `src/main/resources/application.yaml` — gateway routes, filters, and other Spring Cloud Gateway settings go here. Routes are typically defined either in YAML or programmatically via `RouteLocator` beans.

**Key architectural pattern:** Because the gateway is fully reactive, all code in the request/response pipeline must be non-blocking. Use `Mono`/`Flux` (Project Reactor) rather than blocking calls. Custom filters implement `GatewayFilter` or `GlobalFilter`.

**Testing:** Uses JUnit 5 + `reactor-test` for reactive assertions (`StepVerifier`). Integration tests load the full Spring context.