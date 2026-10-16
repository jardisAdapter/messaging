<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Connection;

use Closure;
use JardisSupport\Contract\Messaging\Exception\ConnectionException;
use PDO;

/**
 * External database connection wrapper
 *
 * Wraps an externally managed PDO connection for reuse in messaging.
 * The external system is responsible for connection lifecycle.
 *
 * Takes either a ready PDO handle or a provider `Closure(): PDO`. A provider is
 * invoked lazily on the first {@see getClient()} and its result is reused afterwards
 * (memoized only on success). Construction, {@see connect()}, {@see disconnect()} and
 * {@see isConnected()} never call it. An exception thrown by the provider reaches the
 * caller of {@see getClient()} unchanged and the provider is called again on the next access.
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

    /** @var PDO|Closure(): PDO */
    private PDO|Closure $client;

    /**
     * @param PDO|Closure(): PDO $client Externally managed PDO instance or lazy provider
     * @param bool $manageLifecycle If true, allows disconnect() to close connection (default: false)
     */
    public function __construct(
        PDO|Closure $client,
        private readonly bool $manageLifecycle = false,
    ) {
        $this->client = $client;
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
     * Resolves a lazy provider on first call (exactly once on success).
     *
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

        $client = $this->client;
        if ($client instanceof Closure) {
            $client = $client();
            $this->client = $client;
        }

        return $client;
    }
}
