-- SQL Server schema for order-gateway (Flyway picks db/migration/{vendor}).

CREATE TABLE orders (
    id               VARCHAR(36)       NOT NULL CONSTRAINT pk_orders PRIMARY KEY,
    idempotency_key  VARCHAR(64)       NOT NULL,
    request_hash     VARCHAR(64)        NOT NULL,
    account_id       VARCHAR(32)       NOT NULL,
    instrument       VARCHAR(12)        NOT NULL,
    side             VARCHAR(4)        NOT NULL CONSTRAINT ck_orders_side CHECK (side IN ('BUY', 'SELL')),
    quantity         BIGINT            NOT NULL CONSTRAINT ck_orders_qty CHECK (quantity > 0),
    limit_price      DECIMAL(18, 6)    NOT NULL,
    currency         VARCHAR(3)         NOT NULL,
    status           VARCHAR(16)       NOT NULL,
    reject_reasons   VARCHAR(400)      NULL,
    created_at       DATETIMEOFFSET(6) NOT NULL,
    updated_at       DATETIMEOFFSET(6) NOT NULL,
    version          BIGINT            NOT NULL
);

-- Enforces idempotency even when two first attempts race.
CREATE UNIQUE INDEX ux_orders_idempotency_key ON orders (idempotency_key);
CREATE INDEX ix_orders_account_created ON orders (account_id, created_at DESC) INCLUDE (status);

CREATE TABLE outbox_event (
    id            BIGINT IDENTITY(1, 1) NOT NULL CONSTRAINT pk_outbox_event PRIMARY KEY,
    topic         VARCHAR(128)      NOT NULL,
    message_key   VARCHAR(64)       NOT NULL,
    payload       VARCHAR(4000)     NOT NULL,
    created_at    DATETIMEOFFSET(6) NOT NULL,
    published_at  DATETIMEOFFSET(6) NULL
);

-- The relay only ever scans unpublished rows. A filtered index keeps that scan tiny
-- however large the published history grows.
CREATE INDEX ix_outbox_unpublished ON outbox_event (id) WHERE published_at IS NULL;
