---
name: adapter-messaging
description: Multi-transport messaging (Redis, Kafka, RabbitMQ, Database, InMemory). Use for MessagingService, publishers, consumers.
user-invocable: false
zone: post-active
persona: C
prerequisites: [rules-architecture, rules-patterns]
next: []
---

# MESSAGING_COMPONENT_SKILL
> jardisadapter/messaging | NS: `JardisAdapter\Messaging` | PHP 8.2+

## ARCHITECTURE
```
ConnectionFactory   → broker Connection (redis/kafka/kafkaConsumer/rabbitMq/database + from*())
PublisherFactory    → PublisherInterface (consumes Connection)
ConsumerFactory     → ConsumerInterface (consumes Connection)
MessagePublisher    variadic, order = priority, implements MessagePublisherInterface
MessageConsumer     variadic, order = priority, implements MessageConsumerInterface
MessagingService    lazy wrapper — defers creation until first publish()/consume()
```

## RULES
- Connections are **always injected** — publishers/consumers never create them.
- Facades are immutable: variadic `...PublisherInterface`/`ConsumerInterface`, no `add*()`.
- Constructor order = priority; fallback triggers **only** on `MessageException` subclasses, other exceptions propagate.
- Same connection instance reusable across publisher AND consumer.
- All connections require explicit `connect()`; `getClient()` throws `ConnectionException` otherwise.
- Auto-serialization on publish: `string` as-is · `array` → `MessageValidator` + JSON · `object` must implement `JsonSerializable`.
- Auto-deserialization on consume: valid JSON → array, else string. No toggle.
- Consumers propagate all callback exceptions; state-cleanup before re-throw (see ERROR HANDLING).
- Logging = integration-layer concern (Decorator on `ConsumerInterface`/`PublisherInterface`).
- Graceful shutdown (SIGTERM/SIGINT) auto-enabled on first `consume()`; manual stop via `$consumer->stop()`.
- Transport extensions (ext-redis, ext-rdkafka, ext-amqp) are `suggest`; only PDO + ext-pcntl always available.

## CONNECTION INTERFACES (src/Connection/, adapter-internal)
All extend `JardisSupport\Contract\Connection\ConnectionInterface` (`connect`/`disconnect`/`isConnected`). Kept internal because they expose infrastructure types.

| Interface | Getter |
|-----------|--------|
| `RedisConnectionInterface` | `getClient(): Redis` |
| `KafkaProducerConnectionInterface` | `getClient(): Producer` |
| `KafkaConsumerConnectionInterface` | `getClient(): KafkaConsumer` |
| `RabbitMqConnectionInterface` | `getExchange()`, `getChannel()`, `getConnection()` |
| `DatabaseConnectionInterface` | `getClient(): PDO` |

## FACTORY SIGNATURES
```php
// ConnectionFactory — new connections
$connFactory->redis(string $host, int $port = 6379, ?string $password = null, array $options = []);
$connFactory->kafka(string $brokers, ?string $username = null, ?string $password = null, array $options = []);
$connFactory->kafkaConsumer(string $brokers, string $groupId, ?string $username = null, ?string $password = null, array $options = []);
$connFactory->rabbitMq(string $host, int $port = 5672, string $username = 'guest', string $password = 'guest', array $options = []);
$connFactory->database(string $dsn, ?string $username = null, ?string $password = null, array $options = []);

// ConnectionFactory — wrap existing clients
$connFactory->fromRedis(Redis $redis, bool $manageLifecycle = false);
$connFactory->fromPdo(PDO $pdo, bool $manageLifecycle = false);
$connFactory->fromAmqp(AMQPConnection $connection, string $exchangeName = 'amq.direct', string $exchangeType = AMQP_EX_TYPE_DIRECT, bool $manageLifecycle = false, int $exchangeFlags = AMQP_DURABLE);
$connFactory->fromKafkaProducer(Producer $producer, bool $flushOnDisconnect = false);
$connFactory->fromKafkaConsumer(KafkaConsumer $consumer);

// PublisherFactory
$pubFactory->redis(RedisConnectionInterface $connection, bool $useStreams = false);
$pubFactory->kafka(KafkaProducerConnectionInterface $connection);
$pubFactory->rabbitMq(RabbitMqConnectionInterface $connection);
$pubFactory->database(DatabaseConnectionInterface $connection, DatabaseTransportOptions $options = new DatabaseTransportOptions());
$pubFactory->inMemory(?InMemoryTransport $transport = null);
$pubFactory->getSharedTransport(): InMemoryTransport;
$pubFactory->setSharedTransport(InMemoryTransport $transport): void;

// ConsumerFactory
$conFactory->redis(RedisConnectionInterface $connection, bool $useStreams = false);
$conFactory->kafka(KafkaConsumerConnectionInterface $connection);
$conFactory->rabbitMq(RabbitMqConnectionInterface $connection, string $queueName, array $queueConfig = []);
$conFactory->database(DatabaseConnectionInterface $connection, DatabaseTransportOptions $options = new DatabaseTransportOptions());
$conFactory->inMemory(?InMemoryTransport $transport = null);
$conFactory->getSharedTransport(): InMemoryTransport;
$conFactory->setSharedTransport(InMemoryTransport $transport): void;
```

## EXTERNAL CONNECTIONS — disconnect() BEHAVIOR
| Variant | `disconnect()` |
|---------|----------------|
| `manageLifecycle: false` (Redis / Database / RabbitMQ) | **no-op** — external system owns lifecycle |
| Kafka Producer, `flushOnDisconnect: true` | flush pending + set `connected = false` |
| External Kafka Consumer | always no-op (no lifecycle flag) |

## USAGE
```php
use JardisAdapter\Messaging\{MessagingService, MessagePublisher, MessageConsumer};
use JardisAdapter\Messaging\Factory\{ConnectionFactory, PublisherFactory, ConsumerFactory};
use JardisAdapter\Messaging\Handler\CallbackHandler;

$connFactory = new ConnectionFactory();
$pubFactory  = new PublisherFactory();
$conFactory  = new ConsumerFactory();
$redisConn   = $connFactory->redis('localhost');

// Single publisher
$publisher = new MessagePublisher($pubFactory->redis($redisConn));
$publisher->publish('topic', ['data' => 'value']);

// Fallback chain — same broker, multiple instances (order = priority)
$publisher = new MessagePublisher(
    $pubFactory->redis($connFactory->redis('redis-primary')),   // tried first
    $pubFactory->redis($connFactory->redis('redis-secondary')), // fallback
);

// Consumer
$consumer = new MessageConsumer($conFactory->redis($redisConn));
$consumer->consume('topic', new CallbackHandler(
    fn(string|array $msg, array $meta): bool => true  // true=ACK, false=NACK/requeue
));
$consumer->stop();

// Lazy service (DI)
$messaging = new MessagingService(
    publisherFactory: fn() => new MessagePublisher($pubFactory->redis($redisConn)),
    consumerFactory:  fn() => new MessageConsumer($conFactory->redis($redisConn)),
);
$messaging->publish('events', ['k' => 'v']);
$messaging->consume('events', $handler);
$messaging->getPublisher();  // MessagePublisherInterface (lazy)
$messaging->getConsumer();   // MessageConsumerInterface (lazy)
```

## HANDLER
```php
use JardisSupport\Contract\Messaging\MessageHandlerInterface;

class OrderHandler implements MessageHandlerInterface {
    public function handle(string|array $message, array $metadata): bool { /* ... */ }
}
```
Callback returning `false` → NACK/requeue (RabbitMQ, InMemory) or no ACK (Redis consumer groups).

## ERROR HANDLING — STATE-CLEANUP PER TRANSPORT
Behavior on exception or `false` return from callback. Consistent with `jardisadapter/cache` and `jardisadapter/dbconnection`.

| Transport | Cleanup before re-throw |
|-----------|-------------------------|
| RabbitMQ | NACK + REQUEUE |
| Database | increment `attempts`, record `last_error` |
| InMemory | requeue current + remaining (timestamps preserved); no-op in `peek` mode |
| Redis / Kafka | none (stream position / consumer group offset handles it) |

## DATABASE TRANSPORT (no broker)
PDO-based, supports MySQL / PostgreSQL / SQLite via dialect-aware SQL.

```php
$dbConn = $connFactory->database('mysql:host=localhost;dbname=app', 'user', 'pass');

$publisher = new MessagePublisher($pubFactory->database($dbConn));
$consumer  = new MessageConsumer($conFactory->database($dbConn));

// Point-to-Point (one consumer per event)
$consumer->consume('InvoiceCreated', $handler);

// Fan-Out (groups share event, tracked independently)
$consumer->consume('InvoiceCreated', $emailHandler, ['group' => 'email-service']);
$consumer->consume('InvoiceCreated', $pdfHandler,   ['group' => 'pdf-service']);
$consumer->consume('InvoiceCreated', $auditHandler, ['group' => 'audit-log']);

// External PDO
$dbConn = $connFactory->fromPdo($existingPdo, manageLifecycle: false);
```

### DatabaseTransportOptions
```php
new DatabaseTransportOptions(
    table: 'domain_events',
    subscriptionTable: 'domain_event_subscriptions',
    deleteAfterProcessing: false,   // false = soft delete (processed_at), true = hard delete
    pollingIntervalMs: 1000,
    batchSize: 10,
    maxAttempts: 3,
);
```

### Schema & Dialect
- SQL in `src/Schema/domain_events.sql`: `domain_events` (topic, payload, created_at, processed_at, attempts, last_error) + `domain_event_subscriptions` (event_id, consumer_group, processed_at, attempts, last_error).
- Upsert auto-detects driver: MySQL → `INSERT ... ON DUPLICATE KEY UPDATE`; SQLite/PostgreSQL → `INSERT ... ON CONFLICT ... DO UPDATE`.

### Point-to-Point vs Fan-Out
| Mode | Trigger | Tracking |
|------|---------|----------|
| Point-to-Point | no `group` option | `processed_at` on `domain_events` |
| Fan-Out | `['group' => 'name']` | row per group in `domain_event_subscriptions` |

## REDIS STREAMS (Consumer Groups)
```php
$consumer = new MessageConsumer($conFactory->redis($redisConn, useStreams: true));
$consumer->consume('orders', $handler, [
    'group'    => 'order-processors',  // auto-created if missing
    'consumer' => 'worker-1',          // unique per worker
    'block'    => 5000,                // ms
    'count'    => 1,
]);
// ACK automatic when handler returns true
```

## KAFKA
Producer and consumer connections are distinct; `groupId` belongs to the consumer *connection*, not the consumer.
```php
$kafkaProducer = $connFactory->kafka('kafka:9092');
$kafkaConsumer = $connFactory->kafkaConsumer('kafka:9092', 'order-processors');
$publisher = new MessagePublisher($pubFactory->kafka($kafkaProducer));
$consumer  = new MessageConsumer($conFactory->kafka($kafkaConsumer));
```

## RABBITMQ
```php
$rabbitConn = $connFactory->rabbitMq('localhost', 5672, 'guest', 'guest');
$publisher  = new MessagePublisher($pubFactory->rabbitMq($rabbitConn));
$consumer   = new MessageConsumer($conFactory->rabbitMq($rabbitConn, 'order-queue'));
```

## INMEMORY (Testing)
Regular class (no singleton). Share state by passing the same `InMemoryTransport` instance to both factories — directly, or via `setSharedTransport()`.

```php
use JardisAdapter\Messaging\Transport\InMemoryTransport;

// Option 1: explicit shared transport
$transport = new InMemoryTransport();
$publisher = new MessagePublisher($pubFactory->inMemory($transport));
$consumer  = new MessageConsumer($conFactory->inMemory($transport));

// Option 2: factory-level shared transport
$pubFactory->setSharedTransport($transport);
$conFactory->setSharedTransport($transport);
$publisher = new MessagePublisher($pubFactory->inMemory());  // picks up shared transport
$consumer  = new MessageConsumer($conFactory->inMemory());

// Consumer options
$consumer->consume('topic', $handler, [
    'limit' => 5,     // max messages (default: all)
    'peek'  => true,  // don't remove (default: false)
]);

// Inspection
$transport->getMessageCount('topic');
$transport->peek('topic');
```

## CONSUMER OPTIONS
| Option | Transport(s) | Default | Meaning |
|--------|--------------|---------|---------|
| `max_empty_polls` | Kafka, RabbitMQ | `0` | `0` = unlimited (until `stop()`); int bounds polls |
| `limit` | InMemory | all | max messages processed |
| `peek` | InMemory | `false` | don't remove from queue |
| `group`/`consumer`/`block`/`count` | Redis Streams | — | see REDIS STREAMS |
| `group` | Database | — | activates Fan-Out mode |

## CONSUMER METADATA BY TRANSPORT
```php
// Redis Pub/Sub
['channel' => 'orders', 'timestamp' => 1709..., 'type' => 'pubsub']
// Redis Streams
['id' => '1709...-0', 'stream' => 'orders', 'timestamp' => 1709..., 'type' => 'stream']
// Kafka
['partition' => 0, 'offset' => 42, 'timestamp' => 1709..., 'key' => 'order-1', 'topic' => 'orders', 'type' => 'kafka']
// RabbitMQ
['routing_key' => 'order.created', 'delivery_tag' => 1, 'exchange' => 'amq.topic', 'headers' => [], 'type' => 'rabbitmq']
// Database (Fan-Out adds 'group')
['id' => 42, 'topic' => 'OrderCreated', 'created_at' => '2026-...', 'attempts' => 0, 'transport' => 'database']
// InMemory
['topic' => 'test', 'timestamp' => 1709..., 'type' => 'inmemory', 'index' => 0]
```

## CONNECTIONCONFIG
```php
use JardisAdapter\Messaging\Config\ConnectionConfig;

new ConnectionConfig('localhost', 6379, password: 'secret');
ConnectionConfig::fromEnv('REDIS');                          // REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
ConnectionConfig::fromEnv('KAFKA', ['useStreams' => true]);
ConnectionConfig::fromArray(['host' => 'localhost', 'port' => 6379]);
```

## EXCEPTIONS
All extend `MessageException`.

| Exception | Trigger |
|-----------|---------|
| `PublishException` | publishing failed |
| `ConsumerException` | consuming failed |
| `ConnectionException` | connection issue, incl. `getClient()` before `connect()` |
| `MessageException` | base class |

## LAYER
- **Application:** inject `MessagingService` / `MessagePublisher` / `MessageConsumer`.
- **Infrastructure:** wire connections via `ConnectionFactory`, create transports via `PublisherFactory` / `ConsumerFactory`.
- **Domain:** implements `MessageHandlerInterface` only — NEVER imports messaging classes.
