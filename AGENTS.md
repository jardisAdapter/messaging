# jardisadapter/messaging

Unified messaging API over Redis (Pub/Sub + Streams), Kafka, RabbitMQ, Database (PDO), and InMemory (testing). Three-layer Factory plus immutable Facades with priority-based fallback.

## Usage essentials

- **Three-layer Factory Pattern:** `ConnectionFactory` (creates/wraps connections) → `PublisherFactory`/`ConsumerFactory` (consumes connection interfaces) → `MessagePublisher`/`MessageConsumer` (variadic constructor, constructor order = priority, first = highest). No `addPublisher()`/`addConsumer()` — construct once, never mutate.
- **Connections are always injected** (DIP). Publishers/consumers never create their own. Same connection reusable for publisher AND consumer. `getClient()` throws `ConnectionException` if `connect()` has not been called — no implicit auto-connect.
- **External connections via `from*()`:** `fromRedis($redis, manageLifecycle: false)`, `fromPdo($pdo)`, `fromAmqp($conn, exchangeName: …)`, `fromKafkaProducer($producer, flushOnDisconnect: true)`, `fromKafkaConsumer($consumer)`. With `manageLifecycle: false`, `disconnect()` is a no-op — the external system retains control.
- **Auto-Serialization (publish):** `array` → JSON after `MessageValidator`, `JsonSerializable` object → JSON, `string` as-is. **Auto-Deserialization (consume):** valid JSON → array, otherwise string. No toggle. Handler returns `true` = ACK, `false` = NACK/Requeue.
- **Error handling:** Consumers propagate exceptions, but not before state-cleanup has occurred (RabbitMQ NACK+Requeue, Database attempt increment, InMemory requeue). Publisher fallback to the next Layer triggers ONLY on `MessageException` subclasses — other exceptions (e.g. `JsonException`) propagate immediately. Logging is the caller's responsibility via Decorator.
- **Database transport requires no broker:** `DatabasePublisher`/`DatabaseConsumer` with `DatabaseTransportOptions` (table, pollingIntervalMs, batchSize, maxAttempts). Point-to-Point default; Fan-Out via `['group' => 'email-service']` in consumer options (dialect-aware upsert MySQL/PG/SQLite). `ext-redis`/`ext-rdkafka`/`ext-amqp` in `suggest`, only PDO + `ext-pcntl` required. `consume()` registers SIGTERM/SIGINT automatically.

## Full reference

https://docs.jardis.io/en/adapter/messaging
