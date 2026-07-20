package com.starline.keycloak.artemis;

import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Topic;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.jboss.logging.Logger;

import java.io.Serial;
import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Thread-safe, resilient JMS 2.0 publisher for Keycloak event listeners.
 *
 * <h3>What changed vs. the JMS 1.1 version</h3>
 * <ul>
 *   <li>{@link JMSContext} replaces the separate {@code Connection}/{@code Session}/
 *       {@code MessageProducer} triad — one object now owns the whole lifecycle.</li>
 *   <li>{@link JMSProducer} (obtained fresh per-send via {@code context.createProducer()})
 *       replaces {@code MessageProducer}, using its fluent
 *       {@code setProperty(...).send(destination, body)} API instead of manually building
 *       a {@code TextMessage}.</li>
 *   <li>{@link JMSException} (checked) is gone from the send path — JMS 2.0's simplified
 *       API throws the unchecked {@link JMSRuntimeException} instead, which flattens the
 *       try/catch structure considerably.</li>
 *   <li>Connection start/stop is implicit — a {@code JMSContext} created via
 *       {@code createContext(...)} is auto-started, so there is no {@code conn.start()}
 *       step.</li>
 * </ul>
 *
 * <p><b>Why pooling is still needed:</b> a {@link JMSContext} is explicitly documented as
 * <i>not</i> thread-safe (unlike a plain {@code ConnectionFactory}), so JMS 2.0 does not
 * remove the need for a pool in a concurrent publisher — it just shrinks what each pool
 * slot has to hold. The pooling/retry/backoff architecture is unchanged from the JMS 1.1
 * version.
 */
public final class ArtemisPublisher implements Publisher {

    private static final Logger LOG = Logger.getLogger(ArtemisPublisher.class);

    private final ArtemisConfig config;
    private final ActiveMQConnectionFactory connectionFactory;

    private final Semaphore poolPermits;
    private final Deque<PooledContext> pool;

    private final String topicAddress;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final Random jitterRandom = new SecureRandom();

    // ─────────────────────────────────────────────────────────────────────────

    public ArtemisPublisher(ArtemisConfig config) {
        this.config = config;

        this.connectionFactory = new ActiveMQConnectionFactory(config.brokerUrl());
        this.connectionFactory.setReconnectAttempts(config.transportReconnectAttempts());
        this.connectionFactory.setRetryInterval(config.transportRetryIntervalMs());
        this.connectionFactory.setRetryIntervalMultiplier(config.transportRetryMultiplier());
        this.connectionFactory.setMaxRetryInterval(config.transportMaxRetryIntervalMs());

        int size = config.poolSize();
        this.poolPermits = new Semaphore(size, true); // fair
        this.pool = new ArrayDeque<>(size);

        for (int i = 0; i < size; i++) {
            pool.push(new PooledContext(i));
        }

        this.topicAddress = config.address();

        LOG.infof("Publisher initialised — pool=%d, maxAttempts=%d, broker=%s",
                size, config.publishMaxAttempts(), config.brokerUrl());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Publisher interface
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public void publish(String json, Publisher.MessageProperties properties) {
        if (closed.get()) {
            LOG.warn("publish() called after close() — ignoring");
            return;
        }

        publishWithRetry(json, properties);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;

        // Block until every currently-borrowed slot is returned before tearing down
        // the shared connectionFactory — see acquireSlot()/returnSlot().
        try {
            poolPermits.acquire(config.poolSize());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while waiting for in-flight publishes to finish during close()");
        }

        List<PooledContext> drained = new ArrayList<>(config.poolSize());
        synchronized (pool) {
            drained.addAll(pool);
            pool.clear();
        }
        for (PooledContext slot : drained) {
            slot.destroy();
        }

        try {
            connectionFactory.close();
        } catch (Exception ignored) {
            // Do nothing
        }

        LOG.info("ArtemisPublisher closed");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Retry logic
    // ─────────────────────────────────────────────────────────────────────────

    private void publishWithRetry(String json, Publisher.MessageProperties properties) {
        int maxAttempts = config.publishMaxAttempts();
        long delay = config.publishRetryBaseDelayMs();
        double multiplier = config.publishRetryMultiplier();
        long maxDelay = config.publishRetryMaxDelayMs();
        double jitter = config.publishRetryJitterFactor();

        JMSRuntimeException lastException = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                doPublish(json, properties);
                if (attempt > 1) {
                    LOG.infof("Publish succeeded on attempt %d (eventType=%s)",
                            attempt, properties.eventType());
                }
                return;

            } catch (JMSRuntimeException ex) {
                lastException = ex;

                if (!isTransient(ex)) {
                    LOG.errorf(ex, "Non-retryable JMSRuntimeException on publish (eventType=%s)", properties.eventType());
                    throw new ArtemisPublishException("Publish failed (permanent error)", ex);
                }

                if (attempt == maxAttempts) break; // exhausted — fall through to throw

                long actualDelay = computeDelay(delay, jitter);
                LOG.warnf("Publish attempt %d/%d failed (eventType=%s, retrying in %dms): %s",
                        attempt, maxAttempts, properties.eventType(), actualDelay, ex.getMessage());

                sleep(actualDelay);

                delay = Math.min((long) (delay * multiplier), maxDelay);
            }
        }

        LOG.errorf(lastException, "Publish failed after %d attempts (eventType=%s)",
                maxAttempts, properties.eventType());
        throw new ArtemisPublishException(
                "Publish failed after " + maxAttempts + " attempts", lastException);
    }

    /**
     * Sends one message. Borrows a pool slot, sends, then returns it.
     *
     * <p>Note the whole body is now a single fluent {@link JMSProducer} chain — no
     * {@code TextMessage} construction, no checked {@code JMSException}.
     */
    private void doPublish(String json, Publisher.MessageProperties properties) {
        PooledContext slot = acquireSlot();
        boolean healthy = false;

        try {
            slot.ensureOpen();

            JMSProducer producer = slot.context().createProducer()
                    .setDeliveryMode(jakarta.jms.DeliveryMode.PERSISTENT)
                    .setProperty("eventKind", properties.eventKind())
                    .setProperty("eventType", properties.eventType())
                    .setProperty("realmId", properties.realmId());

            if (properties.realmName() != null) producer.setProperty("realmName", properties.realmName());
            if (properties.clientId() != null) producer.setProperty("clientId", properties.clientId());
            if (properties.userId() != null) producer.setProperty("userId", properties.userId());

            producer.send(slot.topic(), json);
            healthy = true;

        } finally {
            returnSlot(slot, healthy);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pool management
    // ─────────────────────────────────────────────────────────────────────────

    private PooledContext acquireSlot() {
        try {
            boolean acquired = poolPermits.tryAcquire(
                    config.poolAcquireTimeoutMs(), TimeUnit.MILLISECONDS);
            if (!acquired) {
                throw new ArtemisPublishException(
                        "Timed out waiting for a pool slot after " + config.poolAcquireTimeoutMs() + "ms", null);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ArtemisPublishException("Interrupted while waiting for pool slot", ie);
        }

        synchronized (pool) {
            PooledContext slot = pool.poll();
            if (slot == null) {
                poolPermits.release();
                throw new ArtemisPublishException("Pool deque empty despite available permit", null);
            }
            return slot;
        }
    }

    private void returnSlot(PooledContext slot, boolean healthy) {
        if (!healthy) {
            slot.destroy();
            slot = new PooledContext(slot.id());
        }
        synchronized (pool) {
            if (!closed.get()) {
                pool.push(slot);
            } else {
                slot.destroy();
            }
        }
        poolPermits.release();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Transient vs permanent error classification
    // ─────────────────────────────────────────────────────────────────────────

    private static boolean isTransient(JMSRuntimeException ex) {
        String errorCode = ex.getErrorCode();

        if (errorCode != null) {
            return switch (errorCode) {
                case "SECURITY_EXCEPTION",
                     "ACT-CLIENT-SEC-003" -> false;
                case "QUEUE_DOES_NOT_EXIST",
                     "ADDRESS_DOES_NOT_EXIST" -> false;
                default -> true;
            };
        }

        return !(ex instanceof jakarta.jms.JMSSecurityRuntimeException
                || ex instanceof jakarta.jms.InvalidDestinationRuntimeException
                || ex instanceof jakarta.jms.IllegalStateRuntimeException);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private long computeDelay(long base, double jitterFactor) {
        double randomFactor = 1.0 + (jitterFactor > 0 ? jitterRandom.nextDouble() * jitterFactor : 0);
        return Math.max(1L, (long) (base * randomFactor));
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeQuietly(AutoCloseable c) {
        if (c == null) return;
        try {
            c.close();
        } catch (Exception ignored) {
            // Do nothing
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Inner types
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * One slot in the connection pool. Where the JMS 1.1 version held a
     * {@code Connection}/{@code Session}/{@code MessageProducer} triad, this holds a
     * single {@link JMSContext} plus the resolved {@link Topic}. Producers are created
     * fresh per-send via {@code context.createProducer()} — cheap, and the recommended
     * JMS 2.0 pattern (a {@code JMSProducer} is a lightweight, disposable builder, unlike
     * the heavier JMS 1.1 {@code MessageProducer}).
     */
    private final class PooledContext {

        private final int id;

        private JMSContext context;
        private Topic topic;

        private volatile boolean markedBroken = false;

        PooledContext(int id) {
            this.id = id;
        }

        int id() {
            return id;
        }

        JMSContext context() {
            return context;
        }

        Topic topic() {
            return topic;
        }

        void ensureOpen() {
            if (markedBroken || context == null) {
                rebuild();
            }
        }

        private void rebuild() {
            closeQuietly(context);
            context = null;
            topic = null;
            markedBroken = false;

            // createContext(...) opens the connection, session, and starts delivery in
            // one call — no separate conn.start().
            JMSContext ctx = connectionFactory.createContext(
                    config.username(), config.password(), JMSContext.AUTO_ACKNOWLEDGE);

            ctx.setExceptionListener(ex -> {
                LOG.warnf("Pool slot #%d connection error — marking broken: %s", id, ex.getMessage());
                markedBroken = true;
            });

            this.context = ctx;
            this.topic = ctx.createTopic(topicAddress);

            LOG.debugf("Pool slot #%d opened (topic=%s)", id, topicAddress);
        }

        void destroy() {
            closeQuietly(context);
            context = null;
            topic = null;
            LOG.debugf("Pool slot #%d destroyed", id);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────

    public static final class ArtemisPublishException extends RuntimeException {
        @Serial
        private static final long serialVersionUID = 1L;

        public ArtemisPublishException(String message, Exception cause) {
            super(message, cause);
        }
    }
}