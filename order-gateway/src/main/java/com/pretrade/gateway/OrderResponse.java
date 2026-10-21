package com.pretrade.gateway;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import com.pretrade.common.Side;

public record OrderResponse(
        String orderId,
        String accountId,
        String instrument,
        Side side,
        long quantity,
        BigDecimal limitPrice,
        String currency,
        OrderEntity.Status status,
        List<String> rejectReasons,
        Instant createdAt,
        Instant updatedAt) {

    static OrderResponse of(OrderEntity o) {
        List<String> reasons = o.getRejectReasons() == null ? List.of() : Arrays.asList(o.getRejectReasons().split(","));
        return new OrderResponse(o.getId(), o.getAccountId(), o.getInstrument(), o.getSide(), o.getQuantity(),
                o.getLimitPrice(), o.getCurrency(), o.getStatus(), reasons, o.getCreatedAt(), o.getUpdatedAt());
    }
}
