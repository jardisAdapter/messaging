<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Consumer;

use DateTimeImmutable;
use JardisAdapter\Messaging\Config\DatabaseTransportOptions;
use JardisAdapter\Messaging\Connection\DatabaseConnectionInterface;
use JardisSupport\Contract\Connection\ConnectionInterface;
use JardisSupport\Contract\Messaging\ConsumerInterface;
use JardisSupport\Contract\Messaging\Exception\ConsumerException;
use PDO;
use PDOException;

/**
 * Database message consumer
 *
 * Polls a database table for unprocessed messages using PDO.
 * Supports two modes:
 *
 * - **Point-to-Point**: No consumer group — processed_at directly on domain_events.
 *   One consumer per event.
 * - **Fan-Out**: Consumer group specified via options — each group tracks its own
 *   processing status in a subscription table. Multiple groups process the same event.
 *
 * Usage:
 * ```php
 * // Point-to-Point (one consumer)
 * $consumer->consume('InvoiceCreated', $handler);
 *
 * // Fan-Out (multiple consumer groups)
 * $consumer->consume('InvoiceCreated', $handler, ['group' => 'email-service']);
 * ```
 */
class DatabaseConsumer implements ConsumerInterface
{
    private bool $running = false;

    public function __construct(
        private readonly DatabaseConnectionInterface $connection,
        private readonly DatabaseTransportOptions $options = new DatabaseTransportOptions(),
    ) {
    }

    /**
     * Consume messages from the database
     *
     * @param string $topic The topic/event name to consume
     * @param callable $callback Message handler: function(string $message, array $metadata): bool
     * @param array<string, mixed> $options Options:
     *                                       - 'group': Consumer group name for fan-out mode
     */
    public function consume(string $topic, callable $callback, array $options = []): void
    {
        if (!$this->connection->isConnected()) {
            $this->connection->connect();
        }

        $this->running = true;
        $group = isset($options['group']) && is_string($options['group']) ? $options['group'] : null;

        while ($this->running) {
            try {
                $events = $group !== null
                    ? $this->fetchUnprocessedForGroup($topic, $group)
                    : $this->fetchUnprocessed($topic);

                foreach ($events as $event) {
                    if (!$this->running) {
                        break;
                    }

                    $this->processEvent($event, $callback, $group);
                }

                // @phpstan-ignore-next-line - $this->running can be changed by stop()
                if (empty($events) && $this->running) {
                    usleep($this->options->pollingIntervalMs * 1000);
                }
            } catch (PDOException $e) {
                // @phpstan-ignore-next-line - $this->running can be changed externally
                if ($this->running) {
                    throw new ConsumerException(
                        "Failed to consume from database: {$e->getMessage()}",
                        previous: $e
                    );
                }
            }
        }
    }

    /**
     * @inheritDoc
     */
    public function stop(): void
    {
        $this->running = false;
    }

    public function getConnection(): ConnectionInterface
    {
        return $this->connection;
    }

    // -------------------------------------------------------------------------
    // Point-to-Point (no consumer group)
    // -------------------------------------------------------------------------

    /**
     * Fetch unprocessed events (Point-to-Point mode)
     *
     * @return array<int, array<string, mixed>>
     */
    private function fetchUnprocessed(string $topic): array
    {
        $pdo = $this->connection->getClient();
        $table = $this->options->table;

        $stmt = $pdo->prepare(
            "SELECT id, topic, payload, created_at, attempts "
            . "FROM {$table} "
            . "WHERE topic = :topic AND processed_at IS NULL AND attempts < :max_attempts "
            . "ORDER BY id ASC LIMIT :batch_size"
        );

        $stmt->bindValue('topic', $topic, PDO::PARAM_STR);
        $stmt->bindValue('max_attempts', $this->options->maxAttempts, PDO::PARAM_INT);
        $stmt->bindValue('batch_size', $this->options->batchSize, PDO::PARAM_INT);
        $stmt->execute();

        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * Mark an event as processed (Point-to-Point: soft or hard delete)
     */
    private function markProcessed(int $eventId): void
    {
        $pdo = $this->connection->getClient();
        $table = $this->options->table;

        if ($this->options->deleteAfterProcessing) {
            $stmt = $pdo->prepare("DELETE FROM {$table} WHERE id = :id");
        } else {
            $stmt = $pdo->prepare(
                "UPDATE {$table} SET processed_at = :processed_at WHERE id = :id"
            );
            $stmt->bindValue('processed_at', (new DateTimeImmutable())->format('Y-m-d H:i:s.u'));
        }

        $stmt->bindValue('id', $eventId, PDO::PARAM_INT);
        $stmt->execute();
    }

    /**
     * Increment attempt counter (Point-to-Point)
     */
    private function incrementAttempts(int $eventId, string $error): void
    {
        $pdo = $this->connection->getClient();
        $table = $this->options->table;

        $stmt = $pdo->prepare(
            "UPDATE {$table} SET attempts = attempts + 1, last_error = :error WHERE id = :id"
        );

        $stmt->bindValue('error', $error);
        $stmt->bindValue('id', $eventId, PDO::PARAM_INT);
        $stmt->execute();
    }

    // -------------------------------------------------------------------------
    // Fan-Out (consumer group)
    // -------------------------------------------------------------------------

    /**
     * Fetch unprocessed events for a specific consumer group (Fan-Out mode)
     *
     * Uses LEFT JOIN on subscription table: events without a subscription row
     * for this group are unprocessed. Events with a subscription that has
     * processed_at = NULL and attempts < maxAttempts are retryable.
     *
     * @return array<int, array<string, mixed>>
     */
    private function fetchUnprocessedForGroup(string $topic, string $group): array
    {
        $pdo = $this->connection->getClient();
        $table = $this->options->table;
        $subTable = $this->options->subscriptionTable;

        $stmt = $pdo->prepare(
            "SELECT e.id, e.topic, e.payload, e.created_at, COALESCE(s.attempts, 0) AS attempts "
            . "FROM {$table} e "
            . "LEFT JOIN {$subTable} s ON e.id = s.event_id AND s.consumer_group = :group "
            . "WHERE e.topic = :topic "
            . "AND (s.id IS NULL OR (s.processed_at IS NULL AND s.attempts < :max_attempts)) "
            . "ORDER BY e.id ASC LIMIT :batch_size"
        );

        $stmt->bindValue('group', $group, PDO::PARAM_STR);
        $stmt->bindValue('topic', $topic, PDO::PARAM_STR);
        $stmt->bindValue('max_attempts', $this->options->maxAttempts, PDO::PARAM_INT);
        $stmt->bindValue('batch_size', $this->options->batchSize, PDO::PARAM_INT);
        $stmt->execute();

        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * Mark an event as processed for a consumer group (Fan-Out)
     *
     * Inserts or updates the subscription row with processed_at timestamp.
     */
    private function markProcessedForGroup(int $eventId, string $group): void
    {
        $now = (new DateTimeImmutable())->format('Y-m-d H:i:s.u');
        $this->upsertSubscription($eventId, $group, $now, 0, null);
    }

    /**
     * Increment attempt counter for a consumer group (Fan-Out)
     *
     * Uses atomic SQL increment to avoid race conditions with concurrent consumers.
     */
    private function incrementAttemptsForGroup(int $eventId, string $group, string $error): void
    {
        $pdo = $this->connection->getClient();
        $subTable = $this->options->subscriptionTable;
        $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);

        if ($driver === 'mysql') {
            $sql = "INSERT INTO {$subTable} (event_id, consumer_group, processed_at, attempts, last_error) "
                . "VALUES (:event_id, :group, NULL, 1, :error) "
                . "ON DUPLICATE KEY UPDATE attempts = attempts + 1, last_error = VALUES(last_error)";
        } else {
            $sql = "INSERT INTO {$subTable} (event_id, consumer_group, processed_at, attempts, last_error) "
                . "VALUES (:event_id, :group, NULL, 1, :error) "
                . "ON CONFLICT(event_id, consumer_group) DO UPDATE SET "
                . "attempts = {$subTable}.attempts + 1, last_error = excluded.last_error";
        }

        $stmt = $pdo->prepare($sql);
        $stmt->bindValue('event_id', $eventId, PDO::PARAM_INT);
        $stmt->bindValue('group', $group);
        $stmt->bindValue('error', $error);
        $stmt->execute();
    }

    /**
     * Dialect-aware upsert for subscription table
     *
     * Detects PDO driver and generates appropriate SQL:
     * - MySQL: INSERT ... ON DUPLICATE KEY UPDATE
     * - SQLite/PostgreSQL: INSERT ... ON CONFLICT ... DO UPDATE
     */
    private function upsertSubscription(
        int $eventId,
        string $group,
        ?string $processedAt,
        int $attempts,
        ?string $error,
    ): void {
        $pdo = $this->connection->getClient();
        $subTable = $this->options->subscriptionTable;
        $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);

        if ($driver === 'mysql') {
            $sql = "INSERT INTO {$subTable} (event_id, consumer_group, processed_at, attempts, last_error) "
                . "VALUES (:event_id, :group, :processed_at, :attempts, :error) "
                . "ON DUPLICATE KEY UPDATE processed_at = VALUES(processed_at), "
                . "attempts = VALUES(attempts), last_error = VALUES(last_error)";
        } else {
            $sql = "INSERT INTO {$subTable} (event_id, consumer_group, processed_at, attempts, last_error) "
                . "VALUES (:event_id, :group, :processed_at, :attempts, :error) "
                . "ON CONFLICT(event_id, consumer_group) DO UPDATE SET "
                . "processed_at = excluded.processed_at, attempts = excluded.attempts, "
                . "last_error = excluded.last_error";
        }

        $stmt = $pdo->prepare($sql);
        $stmt->bindValue('event_id', $eventId, PDO::PARAM_INT);
        $stmt->bindValue('group', $group);
        $stmt->bindValue('processed_at', $processedAt);
        $stmt->bindValue('attempts', $attempts, PDO::PARAM_INT);
        $stmt->bindValue('error', $error);
        $stmt->execute();
    }

    // -------------------------------------------------------------------------
    // Shared
    // -------------------------------------------------------------------------

    /**
     * Process a single event through the callback
     *
     * @param array<string, mixed> $event
     * @param callable $callback
     * @param string|null $group Consumer group (null = Point-to-Point)
     */
    private function processEvent(array $event, callable $callback, ?string $group): void
    {
        $metadata = [
            'id' => (int) $event['id'],
            'topic' => $event['topic'],
            'created_at' => $event['created_at'],
            'attempts' => (int) $event['attempts'],
            'transport' => 'database',
        ];

        if ($group !== null) {
            $metadata['group'] = $group;
        }

        try {
            $result = $callback($event['payload'], $metadata);

            if ($result) {
                $group !== null
                    ? $this->markProcessedForGroup((int) $event['id'], $group)
                    : $this->markProcessed((int) $event['id']);
            } else {
                $group !== null
                    ? $this->incrementAttemptsForGroup((int) $event['id'], $group, 'Handler returned false')
                    : $this->incrementAttempts((int) $event['id'], 'Handler returned false');
                $this->stop();
            }
        } catch (\Throwable $e) {
            // State-cleanup: increment attempts before re-throwing
            try {
                $group !== null
                    ? $this->incrementAttemptsForGroup((int) $event['id'], $group, $e->getMessage())
                    : $this->incrementAttempts((int) $event['id'], $e->getMessage());
            } catch (\Throwable) {
                // Increment failed — original exception takes priority
            }
            throw $e;
        }
    }
}
