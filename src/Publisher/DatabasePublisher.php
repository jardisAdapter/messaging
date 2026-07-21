<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Publisher;

use DateTimeImmutable;
use JardisAdapter\Messaging\Config\DatabaseTransportOptions;
use JardisAdapter\Messaging\Connection\DatabaseConnectionInterface;
use JardisSupport\Contract\Connection\ConnectionInterface;
use JardisSupport\Contract\Messaging\Exception\PublishException;
use JardisSupport\Contract\Messaging\PublisherInterface;
use PDOException;

/**
 * Database message publisher
 *
 * Stores messages in a database table using PDO.
 * Implements the Transactional Outbox pattern — messages are persisted
 * in the same database as the domain data, eliminating the need
 * for an external message broker.
 */
class DatabasePublisher implements PublisherInterface
{
    public function __construct(
        private readonly DatabaseConnectionInterface $connection,
        private readonly DatabaseTransportOptions $options = new DatabaseTransportOptions(),
    ) {
    }

    /**
     * @inheritDoc
     */
    public function publish(string $topic, string $message, array $options = []): bool
    {
        if (!$this->connection->isConnected()) {
            $this->connection->connect();
        }

        try {
            $pdo = $this->connection->getClient();
            $table = $this->options->table;

            $stmt = $pdo->prepare(
                "INSERT INTO {$table} (topic, payload, created_at) VALUES (:topic, :payload, :created_at)"
            );

            return $stmt->execute([
                'topic' => $topic,
                'payload' => $message,
                'created_at' => (new DateTimeImmutable())->format('Y-m-d H:i:s.u'),
            ]);
        } catch (PDOException $e) {
            throw new PublishException(
                "Failed to publish message to database: {$e->getMessage()}",
                previous: $e
            );
        }
    }

    public function getConnection(): ConnectionInterface
    {
        return $this->connection;
    }
}
