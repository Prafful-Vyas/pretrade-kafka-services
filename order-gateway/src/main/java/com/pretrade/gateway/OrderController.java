package com.pretrade.gateway;

import java.net.URI;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/v1/orders")
@Tag(name = "Orders")
public class OrderController {
    private static final java.util.regex.Pattern KEY = java.util.regex.Pattern.compile("^[A-Za-z0-9-]{8,64}$");

    static class InvalidKeyException extends RuntimeException {
        InvalidKeyException() {
            super("Idempotency-Key must be 8-64 characters of letters, digits or '-'");
        }
    }

    private final OrderService service;

    public OrderController(OrderService service) {
        this.service = service;
    }

    @PostMapping
    @Operation(summary = "Submit an order for pre-trade risk checks",
            description = "Returns 202 Accepted with status PENDING_RISK. Poll GET /{id} or consume orders.decisions. "
                    + "Retrying with the same Idempotency-Key returns the original order (200).")
    @ApiResponse(responseCode = "202", description = "Order accepted for risk checks")
    @ApiResponse(responseCode = "200", description = "Replay of an earlier request with the same key")
    @ApiResponse(responseCode = "400", description = "Invalid order")
    @ApiResponse(responseCode = "409", description = "Idempotency-Key reused with a different order")
    public ResponseEntity<OrderResponse> submit(
            @Parameter(description = "Client-generated unique key, 8-64 chars")
            @RequestHeader("Idempotency-Key") String key,
            @Valid @RequestBody OrderRequest request) {
        if (!KEY.matcher(key).matches()) {
            throw new InvalidKeyException();
        }
        OrderService.Submission s = service.submit(key, request);
        OrderResponse body = OrderResponse.of(s.order());
        if (s.replay()) {
            return ResponseEntity.ok(body);
        }
        return ResponseEntity.accepted().location(URI.create("/api/v1/orders/" + body.orderId())).body(body);
    }

    @GetMapping("/{id}")
    @Operation(summary = "Get an order and its risk decision")
    public ResponseEntity<OrderResponse> get(@PathVariable String id) {
        return service.find(id).map(OrderResponse::of).map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ProblemDetail invalid(MethodArgumentNotValidException ex) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "order failed validation");
        pd.setProperty("errors", ex.getBindingResult().getFieldErrors().stream()
                .collect(java.util.stream.Collectors.toMap(FieldError::getField,
                        fe -> String.valueOf(fe.getDefaultMessage()), (a, b) -> a + "; " + b)));
        return pd;
    }

    @ExceptionHandler(MissingRequestHeaderException.class)
    ProblemDetail missingHeader(MissingRequestHeaderException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getHeaderName() + " header is required");
    }

    @ExceptionHandler(InvalidKeyException.class)
    ProblemDetail invalidKey(RuntimeException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    @ExceptionHandler(OrderService.IdempotencyConflictException.class)
    ProblemDetail conflict(RuntimeException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, ex.getMessage());
    }

    @ExceptionHandler(OrderService.ConcurrentSubmitException.class)
    ProblemDetail concurrent(RuntimeException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, ex.getMessage());
    }
}
