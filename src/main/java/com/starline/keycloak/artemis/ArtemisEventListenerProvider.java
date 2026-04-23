package com.starline.keycloak.artemis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.jboss.logging.Logger;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.admin.AdminEvent;

import java.util.Set;

/**
 * Keycloak {@link EventListenerProvider} that serialises the raw
 * {@link Event} / {@link AdminEvent} objects to JSON and publishes them to
 * the configured Artemis address as MULTICAST (pub/sub) messages.
 *
 * <p>No intermediate DTO or wrapper is used — the JSON body is exactly what
 * Jackson produces from the native Keycloak event objects, preserving every
 * field without information loss.
 *
 * <p>One instance of this class is created per Keycloak request/session by
 * {@link ArtemisEventListenerProviderFactory}.  It is intentionally
 * lightweight: all expensive state (connection, session pool, config, event
 * filter) lives in the factory-level singletons passed in at construction.
 */
public final class ArtemisEventListenerProvider implements EventListenerProvider {

    private static final Logger LOG = Logger.getLogger(ArtemisEventListenerProvider.class);

    /**
     * Static, shared ObjectMapper.
     * <p>
     * {@link ObjectMapper} is thread-safe after configuration; creating a new
     * instance per request (as the previous version did) wastes heap and
     * triggers unnecessary class-loading.
     */
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    public static final String UNKNOWN = "UNKNOWN";

    private final Publisher publisher;
    /**
     * Immutable set built once at factory init.
     * Empty means "allow all events".
     */
    private final Set<String> allowedEventTypes;

    ArtemisEventListenerProvider(Publisher publisher, Set<String> allowedEventTypes) {
        this.publisher = publisher;
        this.allowedEventTypes = allowedEventTypes;
    }

    // ── EventListenerProvider ──────────────────────────────────────────

    /**
     * Called for every user-facing event (LOGIN, LOGOUT, REGISTER, …).
     * Serialises the raw {@link Event} to JSON and publishes it.
     */
    @Override
    public void onEvent(Event event) {
        if (event == null) return;

        LOG.debugf("Artemis: processing user event '%s'", event.getType());
        // Java 21: use var for local type inference
        var eventType = event.getType() != null ? event.getType().name() : UNKNOWN;
        if (isNotAllowed(eventType)) {
            LOG.tracef("Artemis: skipping user event '%s' (not in WEBHOOK_EVENTS_TAKEN)", eventType);
            return;
        }

        try {
            var json = MAPPER.writeValueAsString(event);
            var props = Publisher.MessageProperties.forUserEvent(
                    eventType,
                    event.getRealmId(),
                    event.getClientId(),
                    event.getUserId(),
                    event.getRealmName());

            publisher.publish(json, props);

        } catch (ArtemisPublisher.ArtemisPublishException ex) {
            // Already logged inside ArtemisPublisher; re-log with context here
            LOG.errorf(ex, "Artemis: failed to deliver user event type='%s'", eventType);
        } catch (Exception ex) {
            LOG.errorf(ex, "Artemis: unexpected error on user event type='%s'", eventType);
        }
    }

    /**
     * Called for every admin event (realm config changes, user CRUD, …).
     * Serialises the raw {@link AdminEvent} to JSON and publishes it.
     *
     * <p>The composite event-type string is
     * {@code "<RESOURCE_TYPE>-<OPERATION_TYPE>"} (e.g. {@code "USER-UPDATE"}).
     */
    @Override
    public void onEvent(AdminEvent adminEvent, boolean includeRepresentation) {
        if (adminEvent == null) return;


        // Java 21 switch expression with pattern — concise null-safe name extraction
        var resourceType = adminEvent.getResourceType() != null
                ? adminEvent.getResourceType().name() : UNKNOWN;
        var operationType = adminEvent.getOperationType() != null
                ? adminEvent.getOperationType().name() : UNKNOWN;
        var eventType = "%s-%s".formatted(resourceType, operationType);

        LOG.debugf("Artemis: processing admin event '%s'", eventType);
        if (isNotAllowed(eventType)) {
            LOG.tracef("Artemis: skipping admin event '%s' (not in WEBHOOK_EVENTS_TAKEN)", eventType);
            return;
        }

        try {
            var json = MAPPER.writeValueAsString(adminEvent);
            var props = Publisher.MessageProperties.forAdminEvent(
                    eventType,
                    adminEvent.getRealmId(),
                    adminEvent.getRealmName());


            publisher.publish(json, props);
            LOG.debugf("Artemis: published admin event %s", json);

        } catch (ArtemisPublisher.ArtemisPublishException ex) {
            LOG.errorf(ex, "Artemis: failed to deliver admin event type='%s'", eventType);
        } catch (Exception ex) {
            LOG.errorf(ex, "Artemis: unexpected error on admin event type='%s'", eventType);
        }
    }

    @Override
    public void close() {
        // Lifecycle is owned by the factory singleton; nothing to release here.
    }

    // ── helpers ────────────────────────────────────────────────────────

    private boolean isNotAllowed(String eventType) {
        return !allowedEventTypes.isEmpty() && !allowedEventTypes.contains(eventType);
    }
}