
# Keycloak Artemis Event Provider

A Keycloak EventListenerProvider that publishes Keycloak events to **Apache ActiveMQ Artemis** using JMS pub/sub (MULTICAST) semantics.

This provider is designed to be deployed into Keycloak as a custom SPI extension.

---

## Features

- Publishes Keycloak events to Artemis broker
- Supports durable JMS subscriptions
- Configurable retry mechanism for publishing
- Connection pooling for JMS sessions
- Transport-level reconnect support
- Works with Keycloak 26.x (Quarkus distribution)
- No runtime dependency conflicts (Artemis bundled in provider JAR or external lib mounting)

---

## Deployment

### 1. Build the JAR

```bash
mvn clean package -DskipTests
````

This produces:

```
target/keycloak-artemis-provider-1.0.0.jar
```

or shaded version (if enabled in build):

```
target/keycloak-artemis-provider-1.0.0-all.jar
```

---

### 2. Copy provider to Keycloak

Copy the JAR into Keycloak providers directory:

```bash
cp target/keycloak-artemis-provider-1.0.0.jar /opt/keycloak/providers/
```

Or if using Docker:

```bash
-v ./providers:/opt/keycloak/providers
```

---

---

## Configuration (Environment Variables)

All configuration is loaded via environment variables or system properties.

### Required

| Variable                   | Description                 | Default                 |
| -------------------------- | --------------------------- | ----------------------- |
| `WEBHOOK_ARTEMIS_URL`      | Artemis broker URL          | `tcp://localhost:61616` |
| `WEBHOOK_ARTEMIS_USERNAME` | Broker username             | `artemis`               |
| `WEBHOOK_ARTEMIS_PASSWORD` | Broker password             | `artemis`               |
| `WEBHOOK_ARTEMIS_ADDRESS`  | Destination address (topic) | `keycloak.events`       |

---

### Optional

#### Event Filtering

| Variable               | Description                                              |
| ---------------------- | -------------------------------------------------------- |
| `WEBHOOK_EVENTS_TAKEN` | Comma-separated event types to publish (optional filter) |

---

#### Connection Pool

| Variable                          | Description              | Default |
| --------------------------------- | ------------------------ | ------- |
| `WEBHOOK_POOL_SIZE`               | JMS connection pool size | `5`     |
| `WEBHOOK_POOL_ACQUIRE_TIMEOUT_MS` | Pool acquire timeout     | `5000`  |

---

#### Publish Retry Policy

| Variable                              | Description          | Default |
| ------------------------------------- | -------------------- | ------- |
| `WEBHOOK_PUBLISH_MAX_ATTEMPTS`        | Max retry attempts   | `5`     |
| `WEBHOOK_PUBLISH_RETRY_BASE_DELAY_MS` | Base retry delay     | `200`   |
| `WEBHOOK_PUBLISH_RETRY_MULTIPLIER`    | Backoff multiplier   | `2.0`   |
| `WEBHOOK_PUBLISH_RETRY_MAX_DELAY_MS`  | Max retry delay      | `5000`  |
| `WEBHOOK_PUBLISH_RETRY_JITTER`        | Random jitter factor | `0.3`   |

---

#### Transport Retry Policy (Artemis client)

| Variable                                  | Description                          | Default |
| ----------------------------------------- | ------------------------------------ | ------- |
| `WEBHOOK_TRANSPORT_RECONNECT_ATTEMPTS`    | Reconnect attempts (`-1` = infinite) | `-1`    |
| `WEBHOOK_TRANSPORT_RETRY_INTERVAL_MS`     | Initial retry interval               | `1000`  |
| `WEBHOOK_TRANSPORT_RETRY_MULTIPLIER`      | Retry multiplier                     | `2.0`   |
| `WEBHOOK_TRANSPORT_MAX_RETRY_INTERVAL_MS` | Max retry interval                   | `30000` |

---

## Example Configuration

```bash
WEBHOOK_ARTEMIS_URL=tcp://artemis-broker:61616
WEBHOOK_ARTEMIS_USERNAME=admin
WEBHOOK_ARTEMIS_PASSWORD=admin
WEBHOOK_ARTEMIS_ADDRESS=keycloak.events

WEBHOOK_POOL_SIZE=10
WEBHOOK_PUBLISH_MAX_ATTEMPTS=5
WEBHOOK_PUBLISH_RETRY_BASE_DELAY_MS=200
```

---

## Behavior Notes

### Pub/Sub Semantics

* Uses **MULTICAST topic behavior**
* Every subscriber receives the same event
* Multiple listeners will all process the same event independently

---

### Reliability

* Retry applied on publish failure
* Transport automatically reconnects on broker failure
* Connection pooling avoids session exhaustion

---

## Development Notes

* Java 17 required
* Designed for Keycloak 26.x (Quarkus-based distribution)
* Uses JMS Artemis client
* No Spring dependencies

---

## Docker volume Example

```yaml
volumes:
  - ./providers:/opt/keycloak/providers
```

---

## License

[MIT License](https://opensource.org/licenses/MIT)
