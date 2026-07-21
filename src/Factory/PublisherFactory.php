<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Factory;

use JardisAdapter\Messaging\Config\DatabaseTransportOptions;
use JardisAdapter\Messaging\Connection\DatabaseConnectionInterface;
use JardisAdapter\Messaging\Connection\KafkaProducerConnectionInterface;
use JardisAdapter\Messaging\Connection\RabbitMqConnectionInterface;
use JardisAdapter\Messaging\Connection\RedisConnectionInterface;
use JardisAdapter\Messaging\Publisher\DatabasePublisher;
use JardisAdapter\Messaging\Publisher\InMemoryPublisher;
use JardisAdapter\Messaging\Publisher\KafkaPublisher;
use JardisAdapter\Messaging\Publisher\RabbitMqPublisher;
use JardisAdapter\Messaging\Publisher\RedisPublisher;
use JardisAdapter\Messaging\Transport\InMemoryTransport;
use JardisSupport\Contract\Messaging\PublisherInterface;

/**
 * Factory for creating publisher instances from injected connections
 */
final class PublisherFactory
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
    ): RedisPublisher {
        return new RedisPublisher($connection, $useStreams);
    }

    public function kafka(
        KafkaProducerConnectionInterface $connection
    ): KafkaPublisher {
        return new KafkaPublisher($connection);
    }

    public function rabbitMq(
        RabbitMqConnectionInterface $connection
    ): RabbitMqPublisher {
        return new RabbitMqPublisher($connection);
    }

    public function database(
        DatabaseConnectionInterface $connection,
        DatabaseTransportOptions $options = new DatabaseTransportOptions(),
    ): DatabasePublisher {
        return new DatabasePublisher($connection, $options);
    }

    public function inMemory(?InMemoryTransport $transport = null): PublisherInterface
    {
        return new InMemoryPublisher($transport ?? $this->getSharedTransport());
    }
}
