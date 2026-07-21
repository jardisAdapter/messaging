<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Connection;

use JardisAdapter\Messaging\Config\ConnectionConfig;
use JardisSupport\Contract\Messaging\Exception\ConnectionException;
use Redis;
use RedisException;

/**
 * Redis connection manager
 *
 * Handles connection lifecycle to Redis server
 */
class RedisConnection implements RedisConnectionInterface
{
    private ?Redis $client = null;
    private bool $connected = false;

    public function __construct(
        private readonly ConnectionConfig $config
    ) {
    }

    public function __destruct()
    {
        $this->disconnect();
    }

    /**
     * Establish connection to Redis
     *
     * @throws ConnectionException
     */
    public function connect(): void
    {
        if ($this->connected) {
            return;
        }

        try {
            $this->client = new Redis();

            $connected = $this->client->connect(
                $this->config->host,
                $this->config->port,
                timeout: $this->config->options['timeout'] ?? 2.0
            );

            if (!$connected) {
                if ($this->client !== null) {
                    try {
                        $this->client->close();
                    } catch (RedisException) {
                    }
                    $this->client = null;
                }
                throw new ConnectionException(
                    "Failed to connect to Redis at {$this->config->host}:{$this->config->port}"
                );
            }

            // Authenticate if credentials provided
            if ($this->config->password !== null && $this->config->password !== '') {
                if (!$this->client->auth($this->config->password)) {
                    if ($this->client !== null) {
                        try {
                            $this->client->close();
                        } catch (RedisException) {
                        }
                        $this->client = null;
                    }
                    throw new ConnectionException('Redis authentication failed');
                }
            }

            // Select database if specified
            if (isset($this->config->options['database'])) {
                $this->client->select((int) $this->config->options['database']);
            }

            $this->connected = true;
        } catch (RedisException $e) {
            if ($this->client !== null) {
                try {
                    $this->client->close();
                } catch (RedisException) {
                }
                $this->client = null;
            }
            throw new ConnectionException(
                "Redis connection error: {$e->getMessage()}",
                previous: $e
            );
        }
    }

    /**
     * Close connection to Redis
     */
    public function disconnect(): void
    {
        if ($this->client !== null && $this->connected) {
            try {
                $this->client->close();
            } catch (RedisException) {
                // Ignore close errors
            }
            $this->client = null;
            $this->connected = false;
        }
    }

    /**
     * Check if connected
     */
    public function isConnected(): bool
    {
        return $this->connected && $this->client !== null;
    }

    /**
     * Get the Redis client instance
     *
     * @throws ConnectionException if not connected
     */
    public function getClient(): Redis
    {
        if (!$this->isConnected() || $this->client === null) {
            throw new ConnectionException('Not connected to Redis');
        }

        return $this->client;
    }
}
