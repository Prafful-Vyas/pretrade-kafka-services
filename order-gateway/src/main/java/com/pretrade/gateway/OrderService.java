package com.pretrade.gateway;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pretrade.common.OrderRequested;
import com.pretrade.common.Topics;

@Service
public class OrderService {

    /** Same Idempotency-Key reused for a different order body. */
    public static class IdempotencyConflictException extends RuntimeException {
        IdempotencyConflictException(String key) {
            super("Idempotency-Key " + key + " was already used for a different order");
        }
    }

    public record Submission(OrderEntity order, boolean replay) {
    }

    private final OrderRepository orders;
    private final OutboxRepository outbox;
    private final ObjectMapper json;
    private final Clock clock;

    public OrderService(OrderRepository orders, OutboxRepository outbox, ObjectMapper json, Clock clock) {
        this.orders = orders;
        this.outbox = outbox;
        this.json = json;
        this.clock = clock;
    }

    /**
     * Idempotent submit. A retry with the same key and body returns the original order
     * (the client may have timed out before seeing it). The same key with a different
     * body is a client bug and is refused. A race between two concurrent first
     * attempts is settled by the unique index on idempotency_key.
     */
    @Transactional
    public Submission submit(String idempotencyKey, OrderRequest req) {
        String hash = hash(req);
        Optional<OrderEntity> existing = orders.findByIdempotencyKey(idempotencyKey);
        if (existing.isPresent()) {
            return replay(existing.get(), hash, idempotencyKey);
        }
        var now = clock.instant();
        OrderEntity order = new OrderEntity(UUID.randomUUID().toString(), idempotencyKey, hash, req.accountId(),
                req.instrument(), req.side(), req.quantity(), req.limitPrice(), req.currency(), now);
        try {
            orders.saveAndFlush(order);
        } catch (DataIntegrityViolationException race) {
            throw new ConcurrentSubmitException(idempotencyKey);
        }
        OrderRequested event = new OrderRequested(order.getId(), req.accountId(), req.instrument(), req.side(),
                req.quantity(), req.limitPrice(), req.currency(), now);
        outbox.save(new OutboxEvent(Topics.ORDERS_REQUESTED, req.accountId(), write(event), now));
        return new Submission(order, false);
    }

    /** Raised when a concurrent request with the same key committed first; the client should retry. */
    public static class ConcurrentSubmitException extends RuntimeException {
        ConcurrentSubmitException(String key) {
            super("a concurrent request with Idempotency-Key " + key + " is in progress; retry");
        }
    }

    @Transactional(readOnly = true)
    public Optional<OrderEntity> find(String id) {
        return orders.findById(id);
    }

    private Submission replay(OrderEntity order, String hash, String key) {
        if (!order.getRequestHash().equals(hash)) {
            throw new IdempotencyConflictException(key);
        }
        return new Submission(order, true);
    }

    private String write(Object o) {
        try {
            return json.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private String hash(OrderRequest req) {
        try {
            // Canonical form: BigDecimal normalised so 101.5 and 101.50 are the same order.
            String canonical = String.join("|", req.accountId(), req.instrument(), req.side().name(),
                    Long.toString(req.quantity()), req.limitPrice().stripTrailingZeros().toPlainString(), req.currency());
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
