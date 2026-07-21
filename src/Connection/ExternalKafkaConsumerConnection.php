<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Connection;

use JardisSupport\Contract\Messaging\Exception\ConnectionException;
use RdKafka\KafkaConsumer;

/**
 * Wraps an externally managed KafkaConsumer for reuse in messaging.
 */
class ExternalKafkaConsumerConnection implements KafkaConsumerConnectionInterface
{
    private bool $connected = true;

    public function __construct(
        private readonly KafkaConsumer $client
    ) {
    }

    public function connect(): void
    {
        $this->connected = true;
    }

    public function disconnect(): void
    {
        // No-op: external connection lifecycle is not managed by this adapter
    }

    public function isConnected(): bool
    {
        return $this->connected;
    }

    /**
     * @throws ConnectionException if disconnected
     */
    public function getClient(): KafkaConsumer
    {
        if (!$this->isConnected()) {
            throw new ConnectionException(
                'External Kafka consumer is not available.'
            );
        }

        return $this->client;
    }
}
