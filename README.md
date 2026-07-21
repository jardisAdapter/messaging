# Jardis Messaging

![Build Status](https://github.com/jardisAdapter/messaging/actions/workflows/ci.yml/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE.md)
[![PHP Version](https://img.shields.io/badge/PHP-%3E%3D8.2-777BB4.svg)](https://www.php.net/)
[![PHPStan Level](https://img.shields.io/badge/PHPStan-Level%208-brightgreen.svg)](phpstan.neon)
[![PSR-12](https://img.shields.io/badge/Code%20Style-PSR--12-blue.svg)](phpcs.xml)

> Part of **[Jardis](https://jardis.io)** — the Domain-Driven Design platform for PHP. You model your domain; Jardis generates the production-ready hexagonal code (DTOs, Command/Query handlers, repositories, persistence). This package is part of the open-source foundation that generated code runs on.

A message queue for PHP with a unified API across Redis, Kafka, RabbitMQ, Database, and InMemory transports. `ConnectionFactory` creates transport-specific connections; `PublisherFactory` and `ConsumerFactory` produce typed publishers and consumers from those connections. `MessagePublisher` and `MessageConsumer` are immutable facades that accept one or more transport instances via constructor injection and provide automatic serialization, priority-based failover, and graceful shutdown.

---

## Features

- **4 Transports + InMemory** — Redis (Pub/Sub and Streams), Kafka, RabbitMQ, Database (PDO), InMemory (testing)
- **Unified Publish/Consume API** — `publish(topic, message)` and `consume(topic, handler)` across all transports
- **Immutable Facades** — `MessagePublisher` and `MessageConsumer` via variadic constructor injection
- **Automatic Serialization** — Arrays and objects encoded to JSON on publish, decoded transparently on consume
- **Consumer Groups** — Redis Streams and Kafka for horizontal scaling
- **Priority Failover** — Constructor order determines priority; first healthy transport wins
- **Lazy Connection** — `MessagingService` defers publisher and consumer creation until first use
- **Database Transport** — PDO-based messaging with Point-to-Point and Fan-Out modes, no external broker required
- **External Connections** — Wrap existing Redis, PDO, AMQP, or Kafka clients via `ConnectionFactory::from*()`
- **Message Validation** — Payload validation before transmission via `MessageValidator`
- **Graceful Shutdown** — SIGTERM/SIGINT handling enabled automatically on `consume()`

---

## Installation

```bash
composer require jardisadapter/messaging
```

**Optional extensions** (install only what you need):
- `ext-redis` — Redis Streams/Pub-Sub transport
- `ext-rdkafka` — Apache Kafka transport
- `ext-amqp` — RabbitMQ transport

PDO (Database transport) is always available via `ext-pdo`.

---

## Quick Start

```php
use JardisAdapter\Messaging\MessagePublisher;
use JardisAdapter\Messaging\MessageConsumer;
use JardisAdapter\Messaging\Factory\ConnectionFactory;
use JardisAdapter\Messaging\Factory\PublisherFactory;
use JardisAdapter\Messaging\Factory\ConsumerFactory;
use JardisAdapter\Messaging\Handler\CallbackHandler;

$connFactory = new ConnectionFactory();
$pubFactory  = new PublisherFactory();
$conFactory  = new ConsumerFactory();

// Create and share a Redis connection
$redisConn = $connFactory->redis('localhost', 6379);

// Publish
$publisher = new MessagePublisher($pubFactory->redis($redisConn));
$publisher->publish('orders', ['order_id' => 42, 'total' => 99.99]);

// Consume
$consumer = new MessageConsumer($conFactory->redis($redisConn));
$consumer->consume('orders', new CallbackHandler(function (string|array $message, array $metadata): bool {
    // $message = ['order_id' => 42, 'total' => 99.99]  (auto-deserialized)
    return true; // true = ACK, false = reject/requeue
}));
```

---

## Priority Failover

Constructor order determines priority — first argument is tried first. On `MessageException`, the next transport is used automatically.

```php
$primary = $connFactory->redis('redis-primary');
$secondary = $connFactory->redis('redis-secondary');

$publisher = new MessagePublisher(
    $pubFactory->redis($primary),     // tried first
    $pubFactory->redis($secondary),   // fallback if primary fails
);
$publisher->publish('orders', ['order_id' => 42]);
```

---

## Transports

### Redis

Supports both Pub/Sub (default) and Streams mode.

```php
$redisConn = $connFactory->redis('localhost', 6379);

// Pub/Sub (default)
$publisher = new MessagePublisher($pubFactory->redis($redisConn));

// Streams
$publisher = new MessagePublisher($pubFactory->redis($redisConn, useStreams: true));

// Consumer groups (Streams only)
$consumer = new MessageConsumer($conFactory->redis($redisConn, useStreams: true));
$consumer->consume('orders', $handler, [
    'group'    => 'order-processors',  // auto-created if missing
    'consumer' => 'worker-1',
    'block'    => 5000,
    'count'    => 1,
]);
```

### Kafka

Separate connection types for producer and consumer. Consumer group ID is configured on the connection.

```php
// Producer
$kafkaConn = $connFactory->kafka('kafka:9092');
$publisher = new MessagePublisher($pubFactory->kafka($kafkaConn));
$publisher->publish('invoices', ['invoice_id' => 7], ['key' => 'partition-key']);

// Consumer (groupId is part of connection)
$kafkaConsumerConn = $connFactory->kafkaConsumer('kafka:9092', 'invoice-processor');
$consumer = new MessageConsumer($conFactory->kafka($kafkaConsumerConn));
$consumer->consume('invoices', $handler);
```

### RabbitMQ

Queue-based messaging with automatic ACK/NACK handling.

```php
$rabbitConn = $connFactory->rabbitMq('localhost', 5672, 'guest', 'guest');

$publisher = new MessagePublisher($pubFactory->rabbitMq($rabbitConn));
$publisher->publish('order.created', ['orderId' => 42]);

$consumer = new MessageConsumer($conFactory->rabbitMq($rabbitConn, 'order-queue'));
$consumer->consume('order.created', $handler);
```

### Database (PDO)

No external broker required — uses the application's existing database. Supports MySQL, PostgreSQL, and SQLite.

```php
use JardisAdapter\Messaging\Config\DatabaseTransportOptions;

$dbConn = $connFactory->database('mysql:host=localhost;dbname=app', 'user', 'pass');

$options = new DatabaseTransportOptions(
    table: 'domain_events',
    deleteAfterProcessing: false,  // soft delete (default)
    pollingIntervalMs: 1000,
    batchSize: 10,
    maxAttempts: 3,
);

$publisher = new MessagePublisher($pubFactory->database($dbConn, $options));
$consumer = new MessageConsumer($conFactory->database($dbConn, $options));

// Point-to-Point (default): one consumer per event
$consumer->consume('OrderCreated', $handler);

// Fan-Out: multiple consumer groups process the same event
$consumer->consume('InvoiceCreated', $handler, ['group' => 'email-service']);
$consumer->consume('InvoiceCreated', $handler, ['group' => 'pdf-service']);
```

Schema: `src/Schema/domain_events.sql`

### InMemory (Testing)

Synchronous in-memory transport for unit and integration tests.

```php
use JardisAdapter\Messaging\Transport\InMemoryTransport;

$transport = new InMemoryTransport();

$publisher = new MessagePublisher($pubFactory->inMemory($transport));
$consumer  = new MessageConsumer($conFactory->inMemory($transport));

$publisher->publish('test', ['id' => 1]);
$transport->getMessageCount('test');  // 1

$consumer->consume('test', $handler, ['limit' => 5]);
```

---

## External Connections

Wrap existing connections from legacy systems, DI containers, or frameworks.

```php
// Existing Redis instance
$redisConn = $connFactory->fromRedis($existingRedis, manageLifecycle: false);
$publisher = new MessagePublisher($pubFactory->redis($redisConn));

// Existing PDO instance
$dbConn = $connFactory->fromPdo($existingPdo, manageLifecycle: false);

// Existing AMQP connection
$rabbitConn = $connFactory->fromAmqp($amqpConnection, exchangeName: 'custom');

// Existing Kafka producer/consumer
$kafkaProducerConn = $connFactory->fromKafkaProducer($producer, flushOnDisconnect: true);
$kafkaConsumerConn = $connFactory->fromKafkaConsumer($consumer);
```

When `manageLifecycle: false`, `disconnect()` is a no-op — the external system owns the connection lifecycle.

---

## MessagingService (Lazy Loading)

Defers publisher and consumer creation until first use. Ideal for DI containers.

```php
use JardisAdapter\Messaging\MessagingService;

$messaging = new MessagingService(
    publisherFactory: fn() => new MessagePublisher($pubFactory->redis($redisConn)),
    consumerFactory:  fn() => new MessageConsumer($conFactory->redis($redisConn)),
);

$messaging->publish('notifications', ['type' => 'email', 'to' => 'user@example.com']);
$messaging->consume('notifications', $handler);

$messaging->getPublisher();  // MessagePublisherInterface
$messaging->getConsumer();   // MessageConsumerInterface
```

---

## Error Handling

All exceptions extend `MessageException`:

| Exception | When |
|-----------|------|
| `ConnectionException` | Connection fails, `getClient()` called without `connect()` |
| `PublishException` | Publishing fails, serialization error, validation failure |
| `ConsumerException` | Consumer initialization or polling fails |

**Publisher fallback** only triggers on `MessageException` — other exceptions propagate immediately.

**Consumer state-cleanup** is performed before re-throwing: NACK in RabbitMQ, attempt tracking in Database, requeue in InMemory.

---

## Documentation

Full documentation, guides, and API reference:

**[docs.jardis.io/en/adapter/messaging](https://docs.jardis.io/en/adapter/messaging)**

## License

This package is licensed under the [MIT License](LICENSE.md).

---

**[Jardis](https://jardis.io)** · [Documentation](https://docs.jardis.io) · [Headgent](https://headgent.com)

<!-- BEGIN jardis/dev-skills README block — do not edit by hand -->
## AI-Assisted Development

This package ships with a skill for Claude Code, Cursor, Continue, and Aider. Install it in your consuming project:

```bash
composer require --dev jardis/dev-skills
```

More details: <https://docs.jardis.io/en/skills>
<!-- END jardis/dev-skills README block -->
