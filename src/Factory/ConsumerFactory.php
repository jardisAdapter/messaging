<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Factory;

use JardisAdapter\Messaging\Config\DatabaseTransportOptions;
use JardisAdapter\Messaging\Connection\DatabaseConnectionInterface;
use JardisAdapter\Messaging\Connection\KafkaConsumerConnectionInterface;
use JardisAdapter\Messaging\Connection\RabbitMqConnectionInterface;
use JardisAdapter\Messaging\Connection\RedisConnectionInterface;
use JardisAdapter\Messaging\Consumer\DatabaseConsumer;
use JardisAdapter\Messaging\Consumer\InMemoryConsumer;
use JardisAdapter\Messaging\Consumer\KafkaConsumer;
use JardisAdapter\Messaging\Consumer\RabbitMqConsumer;
use JardisAdapter\Messaging\Consumer\RedisConsumer;
use JardisAdapter\Messaging\Transport\InMemoryTransport;
use JardisSupport\Contract\Messaging\ConsumerInterface;

/**
 * Factory for creating consumer instances from injected connections
 */
final class ConsumerFactory
{
    private ?InMemoryTransport $sharedTransport = null;

    /**
     * Get or create the shared InMemoryTransport instance
     *
     * Ensures publisher and consumer factories share the same transport
     * when using the same factory instance.
     */
    public function getSharedTransport(): InMemoryTransport
    {
        if ($this->sharedTransport === null) {
            $this->sharedTransport = new InMemoryTransport();
        }

        return $this->sharedTransport;
    }

    public function setSharedTransport(InMemoryTransport $transport): void
    {
        $this->sharedTransport = $transport;
    }

    /**
     * @param bool $useStreams Use Redis Streams instead of Pub/Sub (default: false)
     */
    public function redis(
        RedisConnectionInterface $connection,
        bool $useStreams = false
    ): RedisConsumer {
        return new RedisConsumer($connection, $useStreams);
    }

    public function kafka(
        KafkaConsumerConnectionInterface $connection
    ): KafkaConsumer {
        return new KafkaConsumer($connection);
    }

    /**
     * @param array<string, mixed> $queueConfig Queue configuration (flags, arguments)
     */
    public function rabbitMq(
        RabbitMqConnectionInterface $connection,
        string $queueName,
        array $queueConfig = []
    ): RabbitMqConsumer {
        return new RabbitMqConsumer($connection, $queueName, $queueConfig);
    }

    public function database(
        DatabaseConnectionInterface $connection,
        DatabaseTransportOptions $options = new DatabaseTransportOptions(),
    ): DatabaseConsumer {
        return new DatabaseConsumer($connection, $options);
    }

    public function inMemory(?InMemoryTransport $transport = null): ConsumerInterface
    {
        return new InMemoryConsumer($transport ?? $this->getSharedTransport());
    }
}
