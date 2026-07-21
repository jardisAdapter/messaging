<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Connection;

use AMQPChannel;
use AMQPConnection;
use AMQPException;
use AMQPExchange;
use JardisSupport\Contract\Messaging\Exception\ConnectionException;

/**
 * External RabbitMQ connection wrapper
 *
 * Wraps an externally managed AMQP connection for reuse in messaging.
 * The external system is responsible for connection lifecycle.
 *
 * Use case: Legacy applications with existing RabbitMQ connections
 *
 * Example:
 * ```php
 * $legacyConnection = new \AMQPConnection(['host' => 'localhost']);
 * $legacyConnection->connect();
 *
 * $connection = new ExternalRabbitMqConnection($legacyConnection);
 * $publisher = new RabbitMqPublisher($connection);
 * ```
 */
class ExternalRabbitMqConnection implements RabbitMqConnectionInterface
{
    private ?AMQPChannel $channel = null;
    private ?AMQPExchange $exchange = null;
    private bool $connected = false;

    /**
     * @param AMQPConnection $connection Externally managed AMQP connection
     * @param string $exchangeName Exchange name (default: 'amq.direct')
     * @param string $exchangeType Exchange type (default: AMQP_EX_TYPE_DIRECT)
     * @param bool $manageLifecycle If true, allows disconnect() to close connection (default: false)
     * @param int $exchangeFlags Exchange flags (default: AMQP_DURABLE)
     */
    public function __construct(
        private readonly AMQPConnection $connection,
        private readonly string $exchangeName = 'amq.direct',
        private readonly string $exchangeType = AMQP_EX_TYPE_DIRECT,
        private readonly bool $manageLifecycle = false,
        private readonly int $exchangeFlags = AMQP_DURABLE
    ) {
    }

    /**
     * Ensure connection is established and create channel/exchange
     *
     * @throws ConnectionException
     */
    public function connect(): void
    {
        if ($this->connected && $this->connection->isConnected()) {
            return;
        }

        // Reset stale state
        $this->channel = null;
        $this->exchange = null;
        $this->connected = false;

        try {
            // Check if external connection is connected
            if (!$this->connection->isConnected()) {
                throw new ConnectionException(
                    'External AMQP connection is not connected. ' .
                    'The external system must establish connection first.'
                );
            }

            // Create channel
            $this->channel = new AMQPChannel($this->connection);

            // Create/declare exchange
            $this->exchange = new AMQPExchange($this->channel);
            $this->exchange->setName($this->exchangeName);
            $this->exchange->setType($this->exchangeType);
            $this->exchange->setFlags($this->exchangeFlags);
            $this->exchange->declareExchange();

            $this->connected = true;
        } catch (AMQPException $e) {
            throw new ConnectionException(
                "Failed to initialize RabbitMQ exchange: {$e->getMessage()}",
                previous: $e
            );
        }
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
                $this->connection->disconnect();
            } catch (AMQPException) {
                // Ignore disconnect errors
            }
        }

        $this->channel = null;
        $this->exchange = null;
        $this->connected = false;
    }

    /**
     * Check if external connection is still alive
     */
    public function isConnected(): bool
    {
        return $this->connected && $this->connection->isConnected();
    }

    /**
     * Get the underlying AMQP connection
     *
     * Note: This is mainly for compatibility. Prefer getExchange() for publishing.
     *
     * @throws ConnectionException if not connected
     */
    public function getConnection(): AMQPConnection
    {
        if (!$this->isConnected()) {
            throw new ConnectionException(
                'External RabbitMQ connection is not available. ' .
                'The external system must ensure connection health.'
            );
        }

        return $this->connection;
    }

    /**
     * Get the AMQP exchange for publishing
     *
     * @throws ConnectionException if not connected
     */
    public function getExchange(): AMQPExchange
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        if ($this->exchange === null) {
            throw new ConnectionException('Exchange not initialized');
        }

        return $this->exchange;
    }

    /**
     * Get the AMQP channel instance
     *
     * @throws ConnectionException if not connected
     */
    public function getChannel(): AMQPChannel
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        if ($this->channel === null) {
            throw new ConnectionException('Channel not initialized');
        }

        return $this->channel;
    }
}
