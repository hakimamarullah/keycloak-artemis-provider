package com.starline.keycloak.artemis;

import org.jboss.logging.Logger;
import org.keycloak.Config;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Keycloak {@link EventListenerProviderFactory} for the Artemis pub/sub provider.
 *
 * <p>Registered via Java {@code ServiceLoader} (see
 * {@code META-INF/services/org.keycloak.events.EventListenerProviderFactory}).
 *
 * <h3>Lifecycle</h3>
 * <ul>
 *   <li>This factory is a <strong>singleton</strong> for the lifetime of the
 *       Keycloak server process.</li>
 *   <li>It owns the shared {@link Publisher} (and therefore the single
 *       JMS {@link javax.jms.Connection} and the session pool).</li>
 *   <li>{@link #create} is called on every Keycloak request; it returns a
 *       lightweight {@link ArtemisEventListenerProvider} that references the
 *       shared publisher and the pre-built event-filter set.</li>
 * </ul>
 *
 * <h3>Activation</h3>
 * Drop the fat JAR into {@code /opt/keycloak/providers}, restart Keycloak,
 * then:
 * <pre>
 *   Admin UI → Realm Settings → Events → Event listeners → add "artemis-pubsub"
 * </pre>
 *
 * <h3>Configuration (environment variables)</h3>
 * <pre>
 *   WEBHOOK_ARTEMIS_URL          Default: tcp://localhost:61616
 *   WEBHOOK_ARTEMIS_USERNAME     Default: guest
 *   WEBHOOK_ARTEMIS_PASSWORD     Default: guest
 *   WEBHOOK_ARTEMIS_ADDRESS      Default: keycloak.events
 *   WEBHOOK_EVENTS_TAKEN         Optional comma-separated filter, e.g. LOGIN,REGISTER
 * </pre>
 */
public final class ArtemisEventListenerProviderFactory implements EventListenerProviderFactory {

    private static final Logger LOG = Logger.getLogger(ArtemisEventListenerProviderFactory.class);

    /**
     * ID shown in the Keycloak Admin UI event-listener dropdown.
     */
    public static final String PROVIDER_ID = "artemis-pubsub";

    // ── singleton state (written once in init(), read-only after that) ─
    private Publisher publisher;
    /**
     * Immutable set of allowed event-type strings.
     * Built once in {@link #init}; empty means "allow all".
     * Passed by reference to every provider instance — no re-parsing per
     * request.
     */
    private Set<String> allowedEventTypes;

    // ── EventListenerProviderFactory ───────────────────────────────────

    @Override
    public String getId() {
        return PROVIDER_ID;
    }

    /**
     * Called once at Keycloak startup.
     * Reads config, builds the immutable event-filter, and creates the
     * publisher (which connects to Artemis lazily on first publish).
     */
    @Override
    public void init(Config.Scope scope) {
        var config = DefaultArtemisConfig.fromEnv();

        // Build the event-type filter set exactly once
        allowedEventTypes = buildAllowedSet(config.eventsTaken());

        publisher = new ArtemisPublisher(config);

        LOG.infof("""
                        ArtemisEventListenerProvider ready
                          url     : %s
                          address : %s
                          filter  : %s""",
                config.brokerUrl(),
                config.address(),
                allowedEventTypes.isEmpty() ? "<all events>" : allowedEventTypes);
    }

    /**
     * Called after all factories are initialised — nothing extra needed here.
     */
    @Override
    public void postInit(KeycloakSessionFactory factory) {
        // No-op
    }

    /**
     * Creates a lightweight per-request provider.
     * The shared {@code publisher} and {@code allowedEventTypes} are passed
     * by reference; no copying or parsing occurs.
     */
    @Override
    public EventListenerProvider create(KeycloakSession session) {
        return new ArtemisEventListenerProvider(publisher, allowedEventTypes);
    }

    /**
     * Called once at Keycloak shutdown — close the JMS connection cleanly.
     */
    @Override
    public void close() {
        if (publisher != null) {
            publisher.close();
            LOG.info("ArtemisEventListenerProvider: JMS connection closed");
        }
    }

    // ── helpers ────────────────────────────────────────────────────────

    /**
     * Parses a comma-separated event-type filter string into an immutable
     * {@link Set}.  Returns an empty set (= allow all) when the input is
     * null or blank.
     */
    private static Set<String> buildAllowedSet(String taken) {
        if (taken == null || taken.isBlank()) return Collections.emptySet();

        return Arrays.stream(taken.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                // Java 21: Collectors.toUnmodifiableSet() — immutable, no defensive copy needed
                .collect(Collectors.toUnmodifiableSet());
    }
}