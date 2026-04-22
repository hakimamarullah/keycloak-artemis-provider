package com.starline.keycloak.artemis;

public interface Publisher extends AutoCloseable {

    void publish(String json, Publisher.MessageProperties properties);

    @Override
    void close();

    /**
     * Typed JMS property bag.  Java 21 record with a compact constructor
     * that enforces non-null on mandatory fields at construction time.
     *
     * <p>{@code clientId} and {@code userId} are intentionally nullable
     * (absent on admin events).
     */
    record MessageProperties(
            String eventKind,
            String eventType,
            String realmId,
            String clientId,   // nullable
            String userId,
            String realmName// nullable
    ) {
        /**
         * Compact constructor — validates mandatory fields.
         */
        public MessageProperties {
            if (eventKind == null || eventKind.isBlank())
                throw new IllegalArgumentException("eventKind is required");
            if (eventType == null || eventType.isBlank())
                throw new IllegalArgumentException("eventType is required");
            if (realmId == null || realmId.isBlank())
                throw new IllegalArgumentException("realmId is required");
        }

        // ── static factories (replaces the old Builder pattern) ───────

        /**
         * Creates properties for a regular user event.
         */
        public static MessageProperties forUserEvent(
                String eventType, String realmId, String clientId, String userId, String realmName) {
            return new MessageProperties("user", eventType, realmId, clientId, userId, realmName);
        }

        /**
         * Creates properties for an admin event (no clientId / userId).
         */
        public static MessageProperties forAdminEvent(String eventType, String realmId, String realmName) {
            return new MessageProperties("admin", eventType, realmId, null, null, realmName);
        }
    }
}
