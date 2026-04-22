package com.starline.keycloak.artemis;

/**
 * Configuration contract for Artemis publisher.
 *
 * Implementations may come from:
 * - Environment variables (default)
 * - Spring @ConfigurationProperties
 * - Test mocks
 * - External config service
 */
public interface ArtemisConfig {

    // ── core ───────────────────────────────────────────────────────────
    String brokerUrl();
    String username();
    String password();
    String address();
    String eventsTaken(); // nullable

    // ── pool ───────────────────────────────────────────────────────────
    int poolSize();
    long poolAcquireTimeoutMs();

    // ── publish retry ──────────────────────────────────────────────────
    int publishMaxAttempts();
    long publishRetryBaseDelayMs();
    double publishRetryMultiplier();
    long publishRetryMaxDelayMs();
    double publishRetryJitterFactor();

    // ── transport retry (Artemis client) ───────────────────────────────
    int transportReconnectAttempts();
    long transportRetryIntervalMs();
    double transportRetryMultiplier();
    long transportMaxRetryIntervalMs();
}