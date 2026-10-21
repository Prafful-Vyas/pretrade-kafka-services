package com.pretrade.gateway;

import java.math.BigDecimal;
import java.time.Instant;

import com.pretrade.common.Side;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

@Entity
@Table(name = "orders")
public class OrderEntity {

    public enum Status { PENDING_RISK, ACCEPTED, REJECTED }

    @Id
    @Column(length = 36)
    private String id;

    @Column(name = "idempotency_key", nullable = false, unique = true, length = 64)
    private String idempotencyKey;

    /** SHA-256 of the request body, to detect a reused key with a different order. */
    @Column(name = "request_hash", nullable = false, length = 64)
    private String requestHash;

    @Column(name = "account_id", nullable = false, length = 32)
    private String accountId;

    @Column(nullable = false, length = 12)
    private String instrument;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 4)
    private Side side;

    @Column(nullable = false)
    private long quantity;

    @Column(name = "limit_price", nullable = false, precision = 18, scale = 6)
    private BigDecimal limitPrice;

    @Column(nullable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private Status status;

    @Column(name = "reject_reasons", length = 400)
    private String rejectReasons;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Version
    private long version;

    protected OrderEntity() {
    }

    public OrderEntity(String id, String idempotencyKey, String requestHash, String accountId, String instrument,
                       Side side, long quantity, BigDecimal limitPrice, String currency, Instant now) {
        this.id = id;
        this.idempotencyKey = idempotencyKey;
        this.requestHash = requestHash;
        this.accountId = accountId;
        this.instrument = instrument;
        this.side = side;
        this.quantity = quantity;
        this.limitPrice = limitPrice;
        this.currency = currency;
        this.status = Status.PENDING_RISK;
        this.createdAt = now;
        this.updatedAt = now;
    }

    /** Applies a risk decision once; returns false if the order was already decided. */
    public boolean decide(boolean accepted, String reasons, Instant now) {
        if (status != Status.PENDING_RISK) {
            return false;
        }
        status = accepted ? Status.ACCEPTED : Status.REJECTED;
        rejectReasons = accepted ? null : reasons;
        updatedAt = now;
        return true;
    }

    public String getId() { return id; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getRequestHash() { return requestHash; }
    public String getAccountId() { return accountId; }
    public String getInstrument() { return instrument; }
    public Side getSide() { return side; }
    public long getQuantity() { return quantity; }
    public BigDecimal getLimitPrice() { return limitPrice; }
    public String getCurrency() { return currency; }
    public Status getStatus() { return status; }
    public String getRejectReasons() { return rejectReasons; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}
