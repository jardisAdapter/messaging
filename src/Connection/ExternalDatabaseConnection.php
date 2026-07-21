<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Connection;

use JardisSupport\Contract\Messaging\Exception\ConnectionException;
use PDO;

/**
 * External database connection wrapper
 *
 * Wraps an externally managed PDO connection for reuse in messaging.
 * The external system is responsible for connection lifecycle.
 *
 * Example:
 * ```php
 * $existingPdo = new PDO('mysql:host=localhost;dbname=app', 'user', 'pass');
 *
 * $connection = new ExternalDatabaseConnection($existingPdo);
 * $publisher = new DatabasePublisher($connection);
 * ```
 */
class ExternalDatabaseConnection implements DatabaseConnectionInterface
{
    private bool $connected = true;

    /**
     * @param PDO $client Externally managed PDO instance
     * @param bool $manageLifecycle If true, allows disconnect() to close connection (default: false)
     */
    public function __construct(
        private readonly PDO $client,
        private readonly bool $manageLifecycle = false,
    ) {
    }

    public function connect(): void
    {
        $this->connected = true;
    }

    public function disconnect(): void
    {
        if ($this->manageLifecycle) {
            $this->connected = false;
        }
    }

    public function isConnected(): bool
    {
        return $this->connected;
    }

    /**
     * @throws ConnectionException if connection is dead
     */
    public function getClient(): PDO
    {
        if (!$this->isConnected()) {
            throw new ConnectionException(
                'External database connection is not available. '
                . 'The external system must ensure connection health.'
            );
        }

        return $this->client;
    }
}
