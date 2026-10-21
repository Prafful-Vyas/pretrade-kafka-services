package com.pretrade.gateway;

import java.math.BigDecimal;

import com.pretrade.common.Side;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

/** Client order. Business limits are enforced by pretrade-risk; this only rejects malformed input. */
public record OrderRequest(
        @Schema(example = "ACC-10042")
        @NotBlank @Pattern(regexp = "^ACC-[A-Z0-9-]{1,28}$") String accountId,

        @Schema(description = "ISIN", example = "GB0002634946")
        @NotBlank @Pattern(regexp = "^[A-Z]{2}[A-Z0-9]{9}[0-9]$") String instrument,

        @NotNull Side side,

        @Positive @Max(100_000_000) long quantity,

        @Schema(example = "101.25")
        @NotNull @DecimalMin(value = "0", inclusive = false) @Digits(integer = 12, fraction = 6) BigDecimal limitPrice,

        @Schema(example = "GBP")
        @NotBlank @Pattern(regexp = "^[A-Z]{3}$") String currency) {
}
