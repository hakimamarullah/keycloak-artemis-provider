package com.starline.keycloak.artemis;

public final class DefaultArtemisConfig implements ArtemisConfig {

    // ── defaults ───────────────────────────────────────────────────────
    public static final String DEFAULT_URL = "tcp://localhost:61616";
    public static final String DEFAULT_USERNAME = "artemis";
    public static final String DEFAULT_PASSWORD = "artemis";
    public static final String DEFAULT_ADDRESS = "keycloak.events";

    private static final int DEFAULT_POOL_SIZE = 5;
    private static final long DEFAULT_POOL_ACQUIRE_TIMEOUT_MS = 5000;

    private static final int DEFAULT_PUBLISH_MAX_ATTEMPTS = 5;
    private static final long DEFAULT_PUBLISH_RETRY_BASE_DELAY_MS = 200;
    private static final double DEFAULT_PUBLISH_RETRY_MULTIPLIER = 2.0;
    private static final long DEFAULT_PUBLISH_RETRY_MAX_DELAY_MS = 5000;
    private static final double DEFAULT_PUBLISH_RETRY_JITTER = 0.3;

    private static final int DEFAULT_TRANSPORT_RECONNECT_ATTEMPTS = -1;
    private static final long DEFAULT_TRANSPORT_RETRY_INTERVAL_MS = 1000;
    private static final double DEFAULT_TRANSPORT_RETRY_MULTIPLIER = 2.0;
    private static final long DEFAULT_TRANSPORT_MAX_RETRY_INTERVAL_MS = 30000;

    // ── fields ─────────────────────────────────────────────────────────
    private final String brokerUrl;
    private final String username;
    private final String password;
    private final String address;
    private final String eventsTaken;

    private final int poolSize;
    private final long poolAcquireTimeoutMs;

    private final int publishMaxAttempts;
    private final long publishRetryBaseDelayMs;
    private final double publishRetryMultiplier;
    private final long publishRetryMaxDelayMs;
    private final double publishRetryJitterFactor;

    private final int transportReconnectAttempts;
    private final long transportRetryIntervalMs;
    private final double transportRetryMultiplier;
    private final long transportMaxRetryIntervalMs;

    private DefaultArtemisConfig(Builder b) {
        this.brokerUrl = b.brokerUrl;
        this.username = b.username;
        this.password = b.password;
        this.address = b.address;
        this.eventsTaken = b.eventsTaken;

        this.poolSize = b.poolSize;
        this.poolAcquireTimeoutMs = b.poolAcquireTimeoutMs;

        this.publishMaxAttempts = b.publishMaxAttempts;
        this.publishRetryBaseDelayMs = b.publishRetryBaseDelayMs;
        this.publishRetryMultiplier = b.publishRetryMultiplier;
        this.publishRetryMaxDelayMs = b.publishRetryMaxDelayMs;
        this.publishRetryJitterFactor = b.publishRetryJitterFactor;

        this.transportReconnectAttempts = b.transportReconnectAttempts;
        this.transportRetryIntervalMs = b.transportRetryIntervalMs;
        this.transportRetryMultiplier = b.transportRetryMultiplier;
        this.transportMaxRetryIntervalMs = b.transportMaxRetryIntervalMs;
    }

    // ── builder ────────────────────────────────────────────────────────
    public static class Builder {

        private String brokerUrl = DEFAULT_URL;
        private String username = DEFAULT_USERNAME;
        private String password = DEFAULT_PASSWORD;
        private String address = DEFAULT_ADDRESS;
        private String eventsTaken;

        private int poolSize = DEFAULT_POOL_SIZE;
        private long poolAcquireTimeoutMs = DEFAULT_POOL_ACQUIRE_TIMEOUT_MS;

        private int publishMaxAttempts = DEFAULT_PUBLISH_MAX_ATTEMPTS;
        private long publishRetryBaseDelayMs = DEFAULT_PUBLISH_RETRY_BASE_DELAY_MS;
        private double publishRetryMultiplier = DEFAULT_PUBLISH_RETRY_MULTIPLIER;
        private long publishRetryMaxDelayMs = DEFAULT_PUBLISH_RETRY_MAX_DELAY_MS;
        private double publishRetryJitterFactor = DEFAULT_PUBLISH_RETRY_JITTER;

        private int transportReconnectAttempts = DEFAULT_TRANSPORT_RECONNECT_ATTEMPTS;
        private long transportRetryIntervalMs = DEFAULT_TRANSPORT_RETRY_INTERVAL_MS;
        private double transportRetryMultiplier = DEFAULT_TRANSPORT_RETRY_MULTIPLIER;
        private long transportMaxRetryIntervalMs = DEFAULT_TRANSPORT_MAX_RETRY_INTERVAL_MS;

        public Builder brokerUrl(String v) {
            this.brokerUrl = v;
            return this;
        }

        public Builder username(String v) {
            this.username = v;
            return this;
        }

        public Builder password(String v) {
            this.password = v;
            return this;
        }

        public Builder address(String v) {
            this.address = v;
            return this;
        }

        public Builder eventsTaken(String v) {
            this.eventsTaken = v;
            return this;
        }

        public Builder poolSize(int v) {
            this.poolSize = v;
            return this;
        }

        public Builder poolAcquireTimeoutMs(long v) {
            this.poolAcquireTimeoutMs = v;
            return this;
        }

        public Builder publishMaxAttempts(int v) {
            this.publishMaxAttempts = v;
            return this;
        }

        public Builder publishRetryBaseDelayMs(long v) {
            this.publishRetryBaseDelayMs = v;
            return this;
        }

        public Builder publishRetryMultiplier(double v) {
            this.publishRetryMultiplier = v;
            return this;
        }

        public Builder publishRetryMaxDelayMs(long v) {
            this.publishRetryMaxDelayMs = v;
            return this;
        }

        public Builder publishRetryJitterFactor(double v) {
            this.publishRetryJitterFactor = v;
            return this;
        }

        public Builder transportReconnectAttempts(int v) {
            this.transportReconnectAttempts = v;
            return this;
        }

        public Builder transportRetryIntervalMs(long v) {
            this.transportRetryIntervalMs = v;
            return this;
        }

        public Builder transportRetryMultiplier(double v) {
            this.transportRetryMultiplier = v;
            return this;
        }

        public Builder transportMaxRetryIntervalMs(long v) {
            this.transportMaxRetryIntervalMs = v;
            return this;
        }

        public ArtemisConfig build() {
            return new DefaultArtemisConfig(this);
        }
    }

    // ── env factory (uses builder) ─────────────────────────────────────
    public static ArtemisConfig fromEnv() {
        return new Builder()
                .brokerUrl(env("WEBHOOK_ARTEMIS_URL", DEFAULT_URL))
                .username(env("WEBHOOK_ARTEMIS_USERNAME", DEFAULT_USERNAME))
                .password(env("WEBHOOK_ARTEMIS_PASSWORD", DEFAULT_PASSWORD))
                .address(env("WEBHOOK_ARTEMIS_ADDRESS", DEFAULT_ADDRESS))
                .eventsTaken(env("WEBHOOK_EVENTS_TAKEN"))

                .poolSize(envInt("WEBHOOK_POOL_SIZE", DEFAULT_POOL_SIZE))
                .poolAcquireTimeoutMs(envLong("WEBHOOK_POOL_ACQUIRE_TIMEOUT_MS", DEFAULT_POOL_ACQUIRE_TIMEOUT_MS))

                .publishMaxAttempts(envInt("WEBHOOK_PUBLISH_MAX_ATTEMPTS", DEFAULT_PUBLISH_MAX_ATTEMPTS))
                .publishRetryBaseDelayMs(envLong("WEBHOOK_PUBLISH_RETRY_BASE_DELAY_MS", DEFAULT_PUBLISH_RETRY_BASE_DELAY_MS))
                .publishRetryMultiplier(envDouble("WEBHOOK_PUBLISH_RETRY_MULTIPLIER", DEFAULT_PUBLISH_RETRY_MULTIPLIER))
                .publishRetryMaxDelayMs(envLong("WEBHOOK_PUBLISH_RETRY_MAX_DELAY_MS", DEFAULT_PUBLISH_RETRY_MAX_DELAY_MS))
                .publishRetryJitterFactor(envDouble("WEBHOOK_PUBLISH_RETRY_JITTER", DEFAULT_PUBLISH_RETRY_JITTER))

                .transportReconnectAttempts(envInt("WEBHOOK_TRANSPORT_RECONNECT_ATTEMPTS", DEFAULT_TRANSPORT_RECONNECT_ATTEMPTS))
                .transportRetryIntervalMs(envLong("WEBHOOK_TRANSPORT_RETRY_INTERVAL_MS", DEFAULT_TRANSPORT_RETRY_INTERVAL_MS))
                .transportRetryMultiplier(envDouble("WEBHOOK_TRANSPORT_RETRY_MULTIPLIER", DEFAULT_TRANSPORT_RETRY_MULTIPLIER))
                .transportMaxRetryIntervalMs(envLong("WEBHOOK_TRANSPORT_MAX_RETRY_INTERVAL_MS", DEFAULT_TRANSPORT_MAX_RETRY_INTERVAL_MS))

                .build();
    }

    // ── getters ────────────────────────────────────────────────────────
    public String brokerUrl() {
        return brokerUrl;
    }

    public String username() {
        return username;
    }

    public String password() {
        return password;
    }

    public String address() {
        return address;
    }

    public String eventsTaken() {
        return eventsTaken;
    }

    public int poolSize() {
        return poolSize;
    }

    public long poolAcquireTimeoutMs() {
        return poolAcquireTimeoutMs;
    }

    public int publishMaxAttempts() {
        return publishMaxAttempts;
    }

    public long publishRetryBaseDelayMs() {
        return publishRetryBaseDelayMs;
    }

    public double publishRetryMultiplier() {
        return publishRetryMultiplier;
    }

    public long publishRetryMaxDelayMs() {
        return publishRetryMaxDelayMs;
    }

    public double publishRetryJitterFactor() {
        return publishRetryJitterFactor;
    }

    public int transportReconnectAttempts() {
        return transportReconnectAttempts;
    }

    public long transportRetryIntervalMs() {
        return transportRetryIntervalMs;
    }

    public double transportRetryMultiplier() {
        return transportRetryMultiplier;
    }

    public long transportMaxRetryIntervalMs() {
        return transportMaxRetryIntervalMs;
    }

    // ── helpers ────────────────────────────────────────────────────────
    private static String env(String key) {
        String v = System.getenv(key);
        if (v != null && !v.isBlank()) return v;
        v = System.getProperty(key);
        if (v != null && !v.isBlank()) return v;
        return null;
    }

    private static String env(String key, String def) {
        String v = env(key);
        return v != null ? v : def;
    }

    private static int envInt(String key, int def) {
        String v = env(key);
        return v != null ? Integer.parseInt(v) : def;
    }

    private static long envLong(String key, long def) {
        String v = env(key);
        return v != null ? Long.parseLong(v) : def;
    }

    private static double envDouble(String key, double def) {
        String v = env(key);
        return v != null ? Double.parseDouble(v) : def;
    }
}