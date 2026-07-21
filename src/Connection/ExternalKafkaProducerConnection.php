<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Connection;

use JardisSupport\Contract\Messaging\Exception\ConnectionException;
use RdKafka\Producer;

/**
 * Wraps an externally managed Kafka Producer for reuse in messaging.
 */
class ExternalKafkaProducerConnection implements KafkaProducerConnectionInterface
{
    private bool $connected = true;

    public function __construct(
        private readonly Producer $client,
        private readonly bool $flushOnDisconnect = false
    ) {
    }

    public function connect(): void
    {
        $this->connected = true;
    }

    public function disconnect(): void
    {
        if ($this->flushOnDisconnect && $this->connected) {
            $this->client->flush(10000);
            $this->connected = false;
        }
    }

    public function isConnected(): bool
    {
        return $this->connected;
    }

    /**
     * @throws ConnectionException if disconnected
     */
    public function getClient(): Producer
    {
        if (!$this->isConnected()) {
            throw new ConnectionException(
                'External Kafka producer is not available.'
            );
        }

        return $this->client;
    }
}
