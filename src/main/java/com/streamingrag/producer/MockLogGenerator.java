package com.streamingrag.producer;

import com.streamingrag.model.LogLevel;
import com.streamingrag.model.SystemLog;
import net.datafaker.Faker;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Generates realistic synthetic distributed microservice logs using Datafaker.
 */
@Component
public class MockLogGenerator {

    public static final List<String> SERVICES = List.of(
            "payment-gateway",
            "user-auth",
            "inventory-service",
            "frontend-bff"
    );

    private final Faker faker;
    private final Random random;

    public MockLogGenerator() {
        this.faker = new Faker();
        this.random = new Random();
    }

    public SystemLog generateLog() {
        String service = SERVICES.get(random.nextInt(SERVICES.size()));
        return generateLog(service);
    }

    public SystemLog generateLog(String serviceName) {
        // Weighted distribution: 70% INFO, 20% WARN, 10% ERROR
        int roll = random.nextInt(100);
        LogLevel level;
        if (roll < 70) {
            level = LogLevel.INFO;
        } else if (roll < 90) {
            level = LogLevel.WARN;
        } else {
            level = LogLevel.ERROR;
        }
        return generateLog(serviceName, level);
    }

    public SystemLog generateLog(String serviceName, LogLevel level) {
        UUID eventId = UUID.randomUUID();
        Instant timestamp = Instant.now();
        String message = createMessage(serviceName, level);
        String stackTrace = (level == LogLevel.ERROR) ? createStackTrace(serviceName) : null;

        return new SystemLog(eventId, timestamp, serviceName, level, message, stackTrace);
    }

    private String createMessage(String service, LogLevel level) {
        String userId = "usr_" + faker.number().digits(6);
        String orderId = "ord_" + faker.number().digits(8);
        String sku = "SKU-" + faker.bothify("???-####").toUpperCase();
        String ip = faker.internet().ipV4Address();

        return switch (service) {
            case "user-auth" -> switch (level) {
                case INFO -> pickRandom(
                        "User authentication successful for " + userId + " via oauth2-github",
                        "JWT session token issued for " + userId + " with TTL 3600s",
                        "Password reset email dispatched to user profile " + userId,
                        "Multi-factor challenge completed for " + userId
                );
                case WARN -> pickRandom(
                        "Repeated failed login attempt (3/5) for " + userId + " from IP " + ip,
                        "Token validation latency exceeded 350ms for issuer auth.corp.internal",
                        "Session expiring within 60s for user " + userId + " - refresh requested",
                        "Rate limit threshold (80%) reached for endpoint /oauth/token"
                );
                case ERROR -> pickRandom(
                        "Authentication provider LDAP timeout contacting ldap.corp.internal:636",
                        "Invalid cryptographic signature on refresh token for user " + userId,
                        "Database connection pool exhausted during credential lookup: HikariPool-1",
                        "OAuth2 provider callback failed with HTTP 503 Service Unavailable"
                );
            };

            case "payment-gateway" -> switch (level) {
                case INFO -> pickRandom(
                        "Payment authorization approved for " + orderId + " - amount: $" + faker.commerce().price(10, 500),
                        "Refund processed successfully for transaction " + UUID.randomUUID() + " (" + orderId + ")",
                        "Payment webhook delivered to merchant endpoint with HTTP 200",
                        "Tokenized card authorization succeeded for terminal " + faker.number().digits(4)
                );
                case WARN -> pickRandom(
                        "Payment gateway latency warning: Stripe API responded in 1840ms",
                        "Card CVV verification retry required for order " + orderId,
                        "Exchange rate cache expired for currency EUR - fallback to default rate",
                        "High retry count (2/3) encountered for payment settlement batch"
                );
                case ERROR -> pickRandom(
                        "Charge capture declined: insufficient funds for order " + orderId,
                        "Stripe API unreachable after 3 exponential backoff retries (HTTP 502)",
                        "Fatal deadlock on transaction ledger update for order " + orderId,
                        "Cryptographic HSM key rotation failure during settlement signing"
                );
            };

            case "inventory-service" -> switch (level) {
                case INFO -> pickRandom(
                        "Stock reservation confirmed for " + sku + " (qty: " + faker.number().numberBetween(1, 10) + ")",
                        "Warehouse inventory sync completed for facility WH-" + faker.number().digits(2),
                        "Product catalog catalog-v2 reindexed with 150 updated items",
                        "Inbound shipment received and stock incremented for " + sku
                );
                case WARN -> pickRandom(
                        "Low stock warning: " + sku + " remaining units: " + faker.number().numberBetween(1, 5),
                        "Concurrent stock reservation conflict detected for " + sku + " - retrying",
                        "Warehouse inventory delta synchronization delayed by 450ms",
                        "Catalog cache miss ratio elevated to 42%"
                );
                case ERROR -> pickRandom(
                        "Optimistic lock exception on stock decrement for " + sku,
                        "Failed to acquire Redis distributed lock for inventory allocation: key=" + sku,
                        "Database constraint violation: negative stock prevented for " + sku,
                        "Kafka event publishing failed for inventory-updated-topic"
                );
            };

            case "frontend-bff" -> switch (level) {
                case INFO -> pickRandom(
                        "HTTP GET /api/v1/catalog - 200 OK (latency: " + faker.number().numberBetween(15, 80) + "ms)",
                        "Server-Sent Events stream established for client " + userId,
                        "GraphQL query /dashboard executed successfully in 45ms",
                        "Static assets cached and served from edge CDN node eu-west-1"
                );
                case WARN -> pickRandom(
                        "Client request timeout after 5000ms on POST /api/v1/checkout",
                        "High memory heap usage detected in Node.js/JVM worker: 84%",
                        "Slow API response from upstream service inventory-service (1340ms)",
                        "Circuit breaker half-open probe dispatched to payment-gateway"
                );
                case ERROR -> pickRandom(
                        "HTTP 500 Internal Server Error returned: upstream payment-gateway timeout",
                        "Unhandled exception in request filter: Connection refused by upstream auth service",
                        "Circuit breaker OPEN for inventory-service on endpoint /api/v1/cart",
                        "Response serialization failed: buffer overflow in JSON stream writer"
                );
            };

            default -> level + " log event from " + service;
        };
    }

    private String createStackTrace(String service) {
        return switch (service) {
            case "user-auth" -> """
                    javax.naming.CommunicationException: ldap.corp.internal:636
                        at java.naming/com.sun.jndi.ldap.Connection.<init>(Connection.java:250)
                        at java.naming/com.sun.jndi.ldap.LdapClient.<init>(LdapClient.java:137)
                        at com.streamingrag.auth.LdapAuthProvider.authenticate(LdapAuthProvider.java:84)
                        at com.streamingrag.auth.AuthService.login(AuthService.java:42)""";

            case "payment-gateway" -> """
                    java.net.SocketTimeoutException: Read timed out connecting to https://api.stripe.com/v1/charges
                        at java.base/sun.nio.ch.NioSocketImpl.timedRead(NioSocketImpl.java:288)
                        at java.base/sun.nio.ch.NioSocketImpl.implRead(NioSocketImpl.java:314)
                        at com.streamingrag.payment.StripeClient.charge(StripeClient.java:112)
                        at com.streamingrag.payment.PaymentService.processOrder(PaymentService.java:65)""";

            case "inventory-service" -> """
                    org.springframework.dao.OptimisticLockingFailureException: Row was updated or deleted by another transaction
                        at org.hibernate.internal.ExceptionConverterImpl.convert(ExceptionConverterImpl.java:154)
                        at com.streamingrag.inventory.StockRepository.decrement(StockRepository.java:93)
                        at com.streamingrag.inventory.InventoryService.reserveStock(InventoryService.java:51)""";

            case "frontend-bff" -> """
                    org.springframework.web.client.HttpServerErrorException$InternalServerError: 500 Internal Server Error: [Upstream service unavailable]
                        at org.springframework.web.client.HttpServerErrorException.create(HttpServerErrorException.java:102)
                        at com.streamingrag.bff.GatewayClient.forwardRequest(GatewayClient.java:78)
                        at com.streamingrag.bff.BffController.checkout(BffController.java:55)""";

            default -> """
                    java.lang.RuntimeException: Unexpected system failure
                        at com.streamingrag.service.DefaultHandler.execute(DefaultHandler.java:30)""";
        };
    }

    private String pickRandom(String... items) {
        return items[random.nextInt(items.length)];
    }
}
