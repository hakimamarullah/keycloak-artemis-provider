package com.starline.keycloak.artemis;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.jboss.logging.Logger;

import javax.jms.*;
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
 * Thread-safe, resilient JMS publisher for Keycloak event listeners.
 *
 * <h3>Architecture</h3>
 * <ul>
 *   <li><b>Connection pool</b> — a fixed pool of {@link PooledConnection} slots (size
 *       configurable via {@link DefaultArtemisConfig#poolSize()}). Each slot owns one JMS
 *       {@link Connection} and creates {@link Session}/{@link MessageProducer} on first
 *       use. Broken slots are evicted and rebuilt transparently.</li>
 *   <li><b>Publish retry</b> — transient {@link JMSException}s trigger an exponential
 *       backoff retry loop with configurable attempts, base delay, multiplier, max delay,
 *       and random jitter. Permanent errors (security, invalid destination) are NOT
 *       retried.</li>
 *   <li><b>Transport reconnect</b> — delegated entirely to the Artemis client
 *       ({@code setReconnectAttempts(-1)}). The client heals the underlying transport
 *       silently; the pool layer handles JMS-level session invalidation on top.</li>
 * </ul>
 *
 * <h3>Configuration</h3>
 * All tuning knobs live in {@link DefaultArtemisConfig} with documented defaults.
 *
 * <h3>Thread safety</h3>
 * Publish is safe to call from concurrent threads. Each call borrows one pool slot
 * (blocking up to {@link DefaultArtemisConfig#poolAcquireTimeoutMs()} if all slots are busy),
 * sends the message, then returns the slot immediately — even on exception.
 */
public final class ArtemisPublisher implements Publisher {

    private static final Logger LOG = Logger.getLogger(ArtemisPublisher.class);

    private final ArtemisConfig config;
    private final ActiveMQConnectionFactory connectionFactory;

    /**
     * The pool: a bounded stack of available slots.
     * {@link Semaphore} bounds concurrent borrows; {@link Deque} is the LIFO stack
     * (LIFO keeps the most-recently-used slot warm, reducing session churn).
     */
    private final Semaphore poolPermits;
    private final Deque<PooledConnection> pool;

    /**
     * Resolved once and reused; topic identity does not change per connection.
     */
    private volatile String topicAddress;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Thread-safe random for jitter — ThreadLocalRandom isn't great inside lambdas.
     */
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

        // Pre-populate the pool with empty slots (lazily connected).
        for (int i = 0; i < size; i++) {
            pool.push(new PooledConnection(i));
        }

        this.topicAddress = config.address();

        LOG.infof("ArtemisPublisherV2 initialised — pool=%d, maxAttempts=%d, broker=%s",
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

        // Drain every slot that is currently idle in the pool.
        List<PooledConnection> drained = new ArrayList<>(config.poolSize());
        synchronized (pool) {
            drained.addAll(pool);
            pool.clear();
        }
        for (PooledConnection slot : drained) {
            slot.destroy();
        }

        try {
            connectionFactory.close();
        } catch (Exception ignored) {
            // Do nothing
        }

        LOG.info("ArtemisPublisherV2 closed");
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

        JMSException lastException = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                doPublish(json, properties);
                if (attempt > 1) {
                    LOG.infof("Publish succeeded on attempt %d (eventType=%s)",
                            attempt, properties.eventType());
                }
                return;

            } catch (JMSException ex) {
                lastException = ex;

                if (!isTransient(ex)) {
                    LOG.errorf(ex, "Non-retryable JMSException on publish (eventType=%s)", properties.eventType());
                    throw new ArtemisPublishException("Publish failed (permanent error)", ex);
                }

                if (attempt == maxAttempts) break; // exhausted — fall through to throw

                long actualDelay = computeDelay(delay, jitter);
                LOG.warnf("Publish attempt %d/%d failed (eventType=%s, retrying in %dms): %s",
                        attempt, maxAttempts, properties.eventType(), actualDelay, ex.getMessage());

                sleep(actualDelay);

                // Advance delay for next iteration (capped).
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
     * If the slot is broken, it is destroyed (not returned) and rebuilt on next borrow.
     */
    private void doPublish(String json, Publisher.MessageProperties properties) throws JMSException {
        PooledConnection slot = acquireSlot();
        boolean healthy = false;

        try {
            slot.ensureOpen();

            TextMessage msg = slot.session().createTextMessage(json);
            msg.setStringProperty("eventKind", properties.eventKind());
            msg.setStringProperty("eventType", properties.eventType());
            msg.setStringProperty("realmId", properties.realmId());

            if (properties.realmName() != null) msg.setStringProperty("realmName", properties.realmName());
            if (properties.clientId() != null) msg.setStringProperty("clientId", properties.clientId());
            if (properties.userId() != null) msg.setStringProperty("userId", properties.userId());

            slot.producer().send(msg);
            healthy = true;

        } finally {
            returnSlot(slot, healthy);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pool management
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Blocks until a pool slot is available or the acquire timeout expires.
     *
     * @throws ArtemisPublishException if the timeout is exceeded or the thread is interrupted
     */
    private PooledConnection acquireSlot() {
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
            PooledConnection slot = pool.poll();
            if (slot == null) {
                // Semaphore said there's a permit but the deque is empty — should never
                // happen under correct usage, but guard defensively.
                poolPermits.release();
                throw new ArtemisPublishException("Pool deque empty despite available permit", null);
            }
            return slot;
        }
    }

    /**
     * Returns a slot to the pool. If the publish was unhealthy the slot is destroyed
     * and replaced with a fresh (unconnected) slot so capacity is never permanently lost.
     */
    private void returnSlot(PooledConnection slot, boolean healthy) {
        if (!healthy) {
            slot.destroy();
            // Rebuild a fresh empty slot so the pool stays at full capacity.
            slot = new PooledConnection(slot.id());
        }
        synchronized (pool) {
            if (!closed.get()) {
                pool.push(slot);
            } else {
                slot.destroy(); // publisher was closed while this slot was in-flight
            }
        }
        poolPermits.release();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Transient vs permanent error classification
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Returns {@code true} if the exception is worth retrying.
     *
     * <p>Permanent errors (security, invalid destination, illegal state) should not be
     * retried — they will never succeed without a configuration change.
     */
    private static boolean isTransient(JMSException ex) {
        String errorCode = ex.getErrorCode();

        // ActiveMQ / Artemis error codes for permanent conditions.
        // See org.apache.activemq.artemis.api.core.ActiveMQExceptionType
        if (errorCode != null) {
            return switch (errorCode) {
                // Security / auth
                case "SECURITY_EXCEPTION",
                     "ACT-CLIENT-SEC-003" -> false;
                // Destination does not exist and auto-create is off
                case "QUEUE_DOES_NOT_EXIST",
                     "ADDRESS_DOES_NOT_EXIST" -> false;
                // Everything else (connection drop, timeout, IO, etc.) is transient
                default -> true;
            };
        }

        // Fallback: inspect the exception type hierarchy.
        return !(ex instanceof javax.jms.JMSSecurityException
                 || ex instanceof javax.jms.InvalidDestinationException
                 || ex instanceof javax.jms.IllegalStateException);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Computes the actual sleep duration: {@code base * (1 + random * jitterFactor)},
     * ensuring the result is always at least 1ms.
     */
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
     * One slot in the connection pool.
     *
     * <p>Lifecycle: created empty → {@link #ensureOpen()} establishes the JMS connection,
     * session, and producer lazily on first use → {@link #destroy()} tears everything down.
     *
     * <p><b>Not thread-safe by itself</b> — the pool contract guarantees only one thread
     * holds a given slot at a time.
     */
    private final class PooledConnection {

        private final int id;

        private Connection connection;
        private Session session;
        private MessageProducer producer;

        /**
         * Set by the ExceptionListener; checked in ensureOpen() before reuse.
         */
        private volatile boolean markedBroken = false;

        PooledConnection(int id) {
            this.id = id;
        }

        int id() {
            return id;
        }

        Session session() {
            return session;
        }

        MessageProducer producer() {
            return producer;
        }

        /**
         * Ensures this slot has a live connection/session/producer.
         * If the connection was marked broken by the ExceptionListener it is rebuilt.
         */
        void ensureOpen() throws JMSException {
            if (markedBroken || connection == null) {
                rebuild();
            }
        }

        private void rebuild() throws JMSException {
            // Tear down whatever is stale.
            closeQuietly(producer);
            closeQuietly(session);
            closeQuietly(connection);
            producer = null;
            session = null;
            connection = null;
            markedBroken = false;

            Connection conn = null;
            try {
                conn = connectionFactory.createConnection(config.username(), config.password());

                // Mark this slot broken on any async JMS error so it gets rebuilt on next borrow.
                conn.setExceptionListener(ex -> {
                    LOG.warnf("Pool slot #%d connection error — marking broken: %s", id, ex.getMessage());
                    markedBroken = true;
                });

                Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Topic t = sess.createTopic(topicAddress);
                MessageProducer prod = sess.createProducer(t);
                prod.setDeliveryMode(DeliveryMode.PERSISTENT);

                conn.start();

                // Assign atomically (all or nothing).
                this.connection = conn;
                this.session = sess;
                this.producer = prod;

                LOG.debugf("Pool slot #%d opened (topic=%s)", id, topicAddress);

            } catch (JMSException ex) {
                closeQuietly(conn); // prevent leak on partial init
                throw ex;
            }
        }

        /**
         * Closes all JMS resources held by this slot. Idempotent.
         */
        void destroy() {
            closeQuietly(producer);
            closeQuietly(session);
            closeQuietly(connection);
            producer = null;
            session = null;
            connection = null;
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