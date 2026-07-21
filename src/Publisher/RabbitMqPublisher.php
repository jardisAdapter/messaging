<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Publisher;

use JardisAdapter\Messaging\Connection\RabbitMqConnectionInterface;
use JardisSupport\Contract\Connection\ConnectionInterface;
use JardisSupport\Contract\Messaging\PublisherInterface;
use JardisSupport\Contract\Messaging\Exception\PublishException;
use AMQPException;

/**
 * RabbitMQ message publisher
 *
 * Uses AMQP exchange for message publishing
 */
class RabbitMqPublisher implements PublisherInterface
{
    public function __construct(
        private readonly RabbitMqConnectionInterface $connection
    ) {
    }

    /**
     * Publish a message to the specified topic
     *
     * @param string $topic The topic or routing key
     * @param string $message The message payload (already serialized)
     * @param array<string, mixed> $options Publisher-specific options
     * @return bool True on success
     * @throws PublishException
     */
    public function publish(string $topic, string $message, array $options = []): bool
    {
        if (!$this->connection->isConnected()) {
            $this->connection->connect();
        }

        try {
            $exchange = $this->connection->getExchange();

            // Extract message options
            $routingKey = $options['routing_key'] ?? $topic;
            $flags = $options['flags'] ?? AMQP_NOPARAM;
            $attributes = $options['attributes'] ?? [];

            // Set default message attributes
            $messageAttributes = array_merge([
                'delivery_mode' => 2, // Persistent
                'content_type' => 'application/json',
            ], $attributes);

            // Publish message
            $exchange->publish(
                $message,
                $routingKey,
                $flags,
                $messageAttributes
            );

            return true;
        } catch (AMQPException $e) {
            throw new PublishException(
                "Failed to publish message to RabbitMQ: {$e->getMessage()}",
                previous: $e
            );
        }
    }

    public function getConnection(): ConnectionInterface
    {
        return $this->connection;
    }
}
