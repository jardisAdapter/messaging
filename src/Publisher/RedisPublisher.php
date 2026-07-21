<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Publisher;

use JardisAdapter\Messaging\Connection\RedisConnectionInterface;
use JardisSupport\Contract\Connection\ConnectionInterface;
use JardisSupport\Contract\Messaging\PublisherInterface;
use JardisSupport\Contract\Messaging\Exception\PublishException;
use RedisException;

/**
 * Redis message publisher
 *
 * Uses Redis Pub/Sub or Streams for message publishing
 */
class RedisPublisher implements PublisherInterface
{
    public function __construct(
        private readonly RedisConnectionInterface $connection,
        private readonly bool $useStreams = false
    ) {
    }

    /**
     * Publish a message to the specified topic
     *
     * @param string $topic The topic or channel/stream name
     * @param string $message The message payload (already serialized)
     * @param array<string, mixed> $options Adapter-specific options
     * @return bool True on success
     * @throws PublishException
     */
    public function publish(string $topic, string $message, array $options = []): bool
    {
        if (!$this->connection->isConnected()) {
            $this->connection->connect();
        }

        try {
            if ($this->useStreams) {
                return $this->publishToStream($topic, $message, $options);
            }

            return $this->publishToPubSub($topic, $message);
        } catch (RedisException $e) {
            throw new PublishException(
                "Failed to publish message to Redis: {$e->getMessage()}",
                previous: $e
            );
        }
    }

    /**
     * Publish using Redis Pub/Sub
     * @throws RedisException
     */
    private function publishToPubSub(string $channel, string $payload): bool
    {
        $redis = $this->connection->getClient();
        $subscribers = $redis->publish($channel, $payload);
        return $subscribers >= 0; // Returns number of subscribers, 0 is valid
    }

    /**
     * Publish using Redis Streams
     *
     * @param array<string, mixed> $options
     * @throws RedisException
     */
    private function publishToStream(string $stream, string $payload, array $options): bool
    {
        $redis = $this->connection->getClient();

        $fields = ['message' => $payload];

        // Add custom fields if provided
        if (isset($options['fields']) && is_array($options['fields'])) {
            $fields = array_merge($fields, $options['fields']);
        }

        // Add to stream with optional MAXLEN
        if (isset($options['maxlen']) && $options['maxlen'] > 0) {
            $messageId = $redis->xAdd(
                $stream,
                '*', // Auto-generate ID
                $fields,
                $options['maxlen'],
                true // Approximate trimming
            );
        } else {
            $messageId = $redis->xAdd(
                $stream,
                '*', // Auto-generate ID
                $fields
            );
        }

        return $messageId !== false;
    }

    public function getConnection(): ConnectionInterface
    {
        return $this->connection;
    }
}
