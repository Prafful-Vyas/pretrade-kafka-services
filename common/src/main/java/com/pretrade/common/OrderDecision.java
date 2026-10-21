package com.pretrade.common;

import java.time.Instant;
import java.util.List;

/** Published by pretrade-risk for every order request it evaluates. */
public record OrderDecision(
        String orderId,
        String accountId,
        DecisionStatus status,
        List<String> reasons,
        Instant decidedAt) {
}
