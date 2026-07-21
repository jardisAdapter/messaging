<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Connection;

use AMQPChannel;
use AMQPConnection;
use AMQPException;
use AMQPExchange;
use JardisAdapter\Messaging\Config\ConnectionConfig;
use JardisSupport\Contract\Messaging\Exception\ConnectionException;

/**
 * RabbitMQ connection manager
 *
 * Handles connection lifecycle to RabbitMQ broker
 */
class RabbitMqConnection implements RabbitMqConnectionInterface
{
    private ?AMQPConnection $connection = null;
    private ?AMQPChannel $channel = null;
    private ?AMQPExchange $exchange = null;
    private bool $connected = false;
    private string $exchangeType;
    private int $exchangeFlags;

    /**
     * @param ConnectionConfig $config Connection configuration
     * @param string $exchangeName Exchange name
     * @param string $exchangeType Exchange type (default: 'topic')
     * @param int $exchangeFlags Exchange flags (default: AMQP_DURABLE)
     */
    public function __construct(
        private readonly ConnectionConfig $config,
        private readonly string $exchangeName = 'amq.topic',
        string $exchangeType = AMQP_EX_TYPE_TOPIC,
        int $exchangeFlags = AMQP_DURABLE
    ) {
        $this->exchangeType = $exchangeType;
        $this->exchangeFlags = $exchangeFlags;
    }

    public function __destruct()
    {
        $this->disconnect();
    }

    /**
     * Establish connection to RabbitMQ
     *
     * @throws ConnectionException
     */
    public function connect(): void
    {
        if ($this->connected) {
            return;
        }

        try {
            // Create connection
            $credentials = [
                'host' => $this->config->host,
                'port' => $this->config->port,
                'vhost' => $this->config->options['vhost'] ?? '/',
            ];

            if ($this->config->username !== null) {
                $credentials['login'] = $this->config->username;
            }
            if ($this->config->password !== null) {
                $credentials['password'] = $this->config->password;
            }

            $this->connection = new AMQPConnection($credentials);
            $this->connection->connect();

            if (!$this->connection->isConnected()) {
                try {
                    $this->connection->disconnect();
                } catch (\Throwable) {
                }
                $this->connection = null;
                $this->channel = null;
                $this->exchange = null;
                throw new ConnectionException(
                    "Failed to connect to RabbitMQ at {$this->config->host}:{$this->config->port}"
                );
            }

            // Create channel
            $this->channel = new AMQPChannel($this->connection);

            // Create and declare exchange
            $this->exchange = new AMQPExchange($this->channel);
            $this->exchange->setName($this->exchangeName);
            $this->exchange->setType($this->exchangeType);
            $this->exchange->setFlags($this->exchangeFlags);
            $this->exchange->declareExchange();

            $this->connected = true;
        } catch (AMQPException $e) {
            try {
                $this->connection->disconnect();
            } catch (\Throwable) {
            }
            $this->connection = null;
            $this->channel = null;
            $this->exchange = null;
            throw new ConnectionException(
                "RabbitMQ connection error: {$e->getMessage()}",
                previous: $e
            );
        }
    }

    /**
     * Close connection to RabbitMQ
     */
    public function disconnect(): void
    {
        if ($this->connection !== null && $this->connected) {
            try {
                if ($this->connection->isConnected()) {
                    $this->connection->disconnect();
                }
            } catch (\Throwable) {
                // Ignore disconnect errors
            }

            $this->exchange = null;
            $this->channel = null;
            $this->connection = null;
            $this->connected = false;
        }
    }

    /**
     * Check if connected
     */
    public function isConnected(): bool
    {
        return $this->connected
            && $this->connection !== null
            && $this->connection->isConnected();
    }

    /**
     * Get the AMQP exchange instance
     *
     * @throws ConnectionException if not connected
     */
    public function getExchange(): AMQPExchange
    {
        if (!$this->isConnected() || $this->exchange === null) {
            throw new ConnectionException('Not connected to RabbitMQ');
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
        if (!$this->isConnected() || $this->channel === null) {
            throw new ConnectionException('Not connected to RabbitMQ');
        }

        return $this->channel;
    }

    /**
     * Get the AMQP connection instance
     *
     * @throws ConnectionException if not connected
     */
    public function getConnection(): AMQPConnection
    {
        if (!$this->isConnected() || $this->connection === null) {
            throw new ConnectionException('Not connected to RabbitMQ');
        }

        return $this->connection;
    }
}
