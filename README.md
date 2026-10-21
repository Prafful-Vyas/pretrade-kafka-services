# Pre-Trade Risk: Event-Driven Microservices (Java 17 · Spring Boot · Kafka)

Two Spring Boot microservices that put every client order through **pre-trade risk
checks** before it can reach a venue. They communicate only through Kafka, keyed by
account, so all orders for an account are processed in order by a single consumer
thread. That one design decision makes the account-limit check exact without locks.

```
client ──POST /api/v1/orders (Idempotency-Key)──► order-gateway ──┐ one DB transaction:
                                                   (REST, OpenAPI) │  orders row + outbox row
                                                                   ▼
                                                   SQL Server: orders, outbox_event
                                                                   │ OutboxRelay (ordered, acks=all,
                                                                   ▼  idempotent producer)
                         Kafka  orders.requested (key = accountId, 6 partitions)
                                                                   │
                                                                   ▼
            marketdata.reference-prices ──► pretrade-risk (3 consumers) ── malformed ──► orders.requested.DLT
               (compacted)                   restricted list · max quantity · stale/missing price ·
                                             directional price band · max notional · account limit
                                                                   │
                         Kafka  orders.decisions (ACCEPTED / REJECTED + every reason)
                                                                   │
                                                                   ▼
                                    order-gateway DecisionListener → order status (first decision wins)
```

## Engineering points

* **Transactional outbox, no dual write.** The order and its event are saved in one SQL
  transaction. A relay publishes outbox rows in insertion order and marks a row sent
  only after the broker acks (`acks=all`, idempotent producer). It stops at the first
  failure, so a later event for an account can never overtake an earlier one.
* **Exactly-once effect over at-least-once delivery.** The relay can re-send after a
  crash. pretrade-risk de-duplicates by `orderId` (bounded LRU), and the gateway applies
  only the first decision per order.
* **Idempotent REST API.** A retry with the same `Idempotency-Key` and the same order
  (compared by a canonical SHA-256 hash, so `101.25` = `101.250`) returns the original
  order with 200. The same key with a different order returns 409. A unique index
  settles a race between two concurrent first attempts.
* **Risk rules** (`PreTradeRiskEngine`) are a pure function of the order, the limits and
  the state, and every failing rule is reported. The price band is directional:
  buying far *below* the reference only hurts the order's own fill chance, so it is
  not a fat-finger error.
* **Failure handling.** Poison messages go straight to a DLT on the same partition
  (non-retryable), and transient errors get bounded retries. Health and readiness probes
  are exposed, plus Micrometer metrics: a decision-latency timer (p50/p99/p999),
  decisions by status, rejections by reason, duplicates and outbox backlog, all
  scrapeable by Prometheus.
* **Contracts**: OpenAPI 3 (springdoc) with RFC 7807 problem details. Validation errors
  are reported per field.
* **Persistence**: Flyway migrations per vendor (`db/migration/{vendor}`). The SQL Server
  schema uses `DATETIMEOFFSET`, check constraints, an `INCLUDE` index and a **filtered
  index** on unpublished outbox rows. Hibernate `ddl-auto=validate` makes the app refuse
  to start if the schema drifts from the entities.

## Run

```bash
mvn verify                                   # unit + integration tests (embedded Kafka, H2)
MSSQL_SA_PASSWORD='<strong password>' docker compose up --build
# http://localhost:8080/swagger-ui.html
curl -X POST localhost:8080/api/v1/orders -H 'Content-Type: application/json' \
     -H 'Idempotency-Key: 6f1c2a9e-demo-0001' \
     -d '{"accountId":"ACC-10042","instrument":"GB0002634946","side":"BUY","quantity":1500,"limitPrice":101.25,"currency":"GBP"}'
```

`deploy/k8s/` has the OpenShift/Kubernetes Deployment and PodDisruptionBudget. Replicas
are capped at the partition count, the pods run non-root with a read-only root
filesystem, and they are spread across zones.

## What was verified

`mvn verify` (Java 17, Spring Boot 3.3.5, spring-kafka with an embedded KRaft broker):

* **pretrade-risk**: 11 unit tests (every rule, directional band, stale prices,
  out-of-order price updates, all reasons reported, limit boundaries and per-account
  overrides). 2 integration tests on embedded Kafka: duplicate delivery produces one
  decision, cumulative exposure rejects the order after the limit is reached, and a
  malformed message lands on the DLT with exception headers.
* **order-gateway**: 6 integration tests on embedded Kafka + H2 (MSSQLServer mode)
  through MockMvc. They cover REST → outbox → Kafka with the account key, decision
  consumption with first-decision-wins, idempotent retry published once, 409 on key
  reuse, per-field validation errors, a missing or invalid key, 404, and the OpenAPI
  document.
* **Not verified**: the SQL Server migration has not been run against a real SQL
  Server (tests use the H2 equivalent), and `docker compose` / k8s manifests were not
  run here (no Docker on the build machine).
