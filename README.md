# TradeExecutionAggregationService

A Kafka Streams microservice in the Africane Order Management System (OMS). It consolidates the partial fills (executions) received from the market for an equity order into a single aggregated record, so downstream services have one consistent view of how much of an order has been executed and at what fees.

## How it works

```
oms.equity.trade.executionsfees  ──►  group by order ID  ──►  aggregate  ──►  oms.equity.order.aggregated-executed-quantity
(TradeExecutionWithFeesEvent)                                             (TradeExecutionAggregationEvent)
```

1. Consumes `TradeExecutionWithFeesEvent` messages from `oms.equity.trade.executionsfees` (key: order UUID).
2. Groups them by key and cogroups into a `TradeExecutionAggregationEvent`, appending each execution (trade ID, price, quantity, side, ticker, dates, counterparty, status, etc.) to the order's list.
3. Publishes the running aggregate for each order to `oms.equity.order.aggregated-executed-quantity`.

Messages that fail deserialization are logged and skipped (`LogAndContinueExceptionHandler`). Event types come from the shared `oms-events-commons` library.

## Tech stack

- Java 21, Spring Boot, Maven (wrapper included)
- Spring Kafka / Kafka Streams, Spring Cloud Stream (Kafka Streams binder)
- Consul (config and discovery), Spring Boot Actuator, Prometheus metrics, Spring Boot Admin client
- GraalVM native image support

## Configuration

| Property | Default | Notes |
|---|---|---|
| `server.port` | `6080` | HTTP port |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094,localhost:9095,localhost:9096` | Kafka brokers |
| `spring.kafka.streams.application-id` | `TradeExecutionAggregationService4` | Kafka Streams app ID |
| `spring.kafka.streams.state-dir` | `/tmp/kafka-streams/` | Local state store location |
| `streamGroupInstance` | `localhost` | Static group instance ID for the consumer |

Profiles: `local` (IntelliJ with Docker Desktop infrastructure), `docker`, `kubernetes`.

## Running

Prerequisites: JDK 21, a reachable Kafka cluster, and access to the private Maven repository that hosts `oms-events-commons` and `oms-kafka-util`.

```bash
./mvnw clean install -DskipTests
./mvnw spring-boot:run -Dspring-boot.run.profiles=local
```

### Docker

```bash
./docker-local.sh   # build the JAR and a local Docker image
./docker.sh         # build the native image and Docker image
```

### CI

`.drone.yml` builds the GraalVM native image on pushes to `main`, `develop`, `staging` and `test`.

## Monitoring

Actuator endpoints are exposed, including health (with probes) and Prometheus metrics.
