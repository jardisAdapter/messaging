<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Connection;

use JardisSupport\Contract\Messaging\Exception\ConnectionException;
use Redis;
use RedisException;

/**
 * External Redis connection wrapper
 *
 * Wraps an externally managed Redis connection for reuse in messaging.
 * The external system is responsible for connection lifecycle.
 *
 * Use case: Legacy applications with existing Redis connections
 *
 * Example:
 * ```php
 * $legacyRedis = new Redis();
 * $legacyRedis->connect('localhost', 6379);
 *
 * $connection = new ExternalRedisConnection($legacyRedis);
 * $publisher = new RedisPublisher($connection);
 * ```
 */
class ExternalRedisConnection implements RedisConnectionInterface
{
    private bool $connected = true;

    /**
     * @param Redis $client Externally managed Redis instance
     * @param bool $manageLifecycle If true, allows disconnect() to close connection (default: false)
     */
    public function __construct(
        private readonly Redis $client,
        private readonly bool $manageLifecycle = false
    ) {
    }

    /**
     * No-op for external connections - already connected
     */
    public function connect(): void
    {
        // External connection is already established
        $this->connected = true;
    }

    /**
     * Optionally close connection based on lifecycle management
     *
     * Note: If manageLifecycle is false, this will not close the external connection
     * as it may still be used by the external system.
     */
    public function disconnect(): void
    {
        if (!$this->manageLifecycle) {
            return;
        }

        if ($this->connected) {
            try {
                $this->client->close();
            } catch (RedisException) {
                // Ignore close errors
            }
        }

        $this->connected = false;
    }

    /**
     * Check if external connection is still alive
     *
     * Performs a health check by pinging Redis
     */
    public function isConnected(): bool
    {
        if (!$this->connected) {
            return false;
        }

        // Health check: Try to ping Redis
        try {
            return $this->client->ping() !== false;
        } catch (RedisException) {
            return false;
        }
    }

    /**
     * Get the external Redis client
     *
     * @throws ConnectionException if connection is dead
     */
    public function getClient(): Redis
    {
        if (!$this->isConnected()) {
            throw new ConnectionException(
                'External Redis connection is not available. ' .
                'The external system must ensure connection health.'
            );
        }

        return $this->client;
    }
}
