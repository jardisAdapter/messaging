<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Consumer;

use JardisAdapter\Messaging\Connection\RedisConnectionInterface;
use JardisSupport\Contract\Connection\ConnectionInterface;
use JardisSupport\Contract\Messaging\ConsumerInterface;
use JardisSupport\Contract\Messaging\Exception\ConsumerException;
use JsonException;
use RedisException;

/**
 * Redis message consumer
 *
 * Supports both Pub/Sub and Streams consumption
 */
class RedisConsumer implements ConsumerInterface
{
    private bool $running = false;

    public function __construct(
        private readonly RedisConnectionInterface $connection,
        private readonly bool $useStreams = false,
    ) {
    }

    /**
     * @inheritDoc
     */
    public function consume(string $topic, callable $callback, array $options = []): void
    {
        if (!$this->connection->isConnected()) {
            $this->connection->connect();
        }

        $this->running = true;

        if ($this->useStreams) {
            $this->consumeFromStream($topic, $callback, $options);
        } else {
            $this->consumeFromPubSub($topic, $callback, $options);
        }
    }

    /**
     * @inheritDoc
     */
    public function stop(): void
    {
        $this->running = false;
    }

    /**
     * Consume from Redis Pub/Sub
     *
     * @param string $channel Channel name
     * @param callable $callback Message handler
     * @param array<string, mixed> $options Options
     * @throws ConsumerException
     */
    private function consumeFromPubSub(string $channel, callable $callback, array $options): void
    {
        $redis = $this->connection->getClient();
        $caughtException = null;

        try {
            $redis->subscribe([$channel], function ($redis, $chan, $message) use ($callback, &$caughtException) {
                if (!$this->running) {
                    return false;
                }

                $metadata = [
                    'channel' => $chan,
                    'timestamp' => time(),
                    'type' => 'pubsub'
                ];

                try {
                    $continue = $callback($message, $metadata);
                } catch (\Throwable $e) {
                    $caughtException = $e;
                    $this->stop();
                    return false;
                }

                if (!$continue) {
                    $this->stop();
                }

                return $this->running;
            });
        } catch (RedisException $e) {
            throw new ConsumerException(
                "Failed to consume from Redis Pub/Sub: {$e->getMessage()}",
                previous: $e
            );
        }

        if ($caughtException !== null) {
            throw $caughtException;
        }
    }

    /**
     * Consume from Redis Streams
     *
     * @param string $stream Stream name
     * @param callable $callback Message handler
     * @param array<string, mixed> $options Options (start_id, block, group, consumer)
     * @throws ConsumerException
     */
    private function consumeFromStream(string $stream, callable $callback, array $options): void
    {
        $redis = $this->connection->getClient();
        $lastId = $options['start_id'] ?? '0';
        $blockMs = $options['block'] ?? 5000;
        $group = $options['group'] ?? null;
        $consumer = $options['consumer'] ?? null;

        // If using consumer groups
        if ($group !== null && $consumer !== null) {
            $this->consumeWithConsumerGroup($stream, $group, $consumer, $callback, $options);
            return;
        }

        // Simple stream reading
        while ($this->running) {
            try {
                $messages = $redis->xRead([$stream => $lastId], 1, $blockMs);

                if (!$messages || !isset($messages[$stream])) {
                    continue;
                }

                foreach ($messages[$stream] as $id => $fields) {
                    $metadata = [
                        'id' => $id,
                        'stream' => $stream,
                        'timestamp' => time(),
                        'type' => 'stream'
                    ];

                    $message = $fields['message'] ?? json_encode($fields, JSON_THROW_ON_ERROR);

                    $continue = $callback($message, $metadata);
                    if ($continue) {
                        $lastId = $id;
                    } else {
                        // Callback returned false - stop consuming
                        $this->stop();
                        break;
                    }
                }
            } catch (RedisException | JsonException $e) {
                // @phpstan-ignore-next-line - $this->running can be changed externally
                if ($this->running) {
                    throw new ConsumerException(
                        "Failed to consume from Redis stream: {$e->getMessage()}",
                        previous: $e
                    );
                }
            }
        }
    }

    /**
     * Consume using Redis Consumer Groups
     *
     * @param string $stream Stream name
     * @param string $group Consumer group name
     * @param string $consumer Consumer name
     * @param callable $callback Message handler
     * @param array<string, mixed> $options Options
     */
    private function consumeWithConsumerGroup(
        string $stream,
        string $group,
        string $consumer,
        callable $callback,
        array $options
    ): void {
        $redis = $this->connection->getClient();
        $blockMs = $options['block'] ?? 5000;
        $count = $options['count'] ?? 1;

        // Create consumer group if it doesn't exist
        try {
            $redis->xGroup('CREATE', $stream, $group, '0', true);
        } catch (RedisException) {
            // Group might already exist
        }

        while ($this->running) {
            try {
                $messages = $redis->xReadGroup($group, $consumer, [$stream => '>'], $count, $blockMs);

                if (!$messages || !isset($messages[$stream])) {
                    continue;
                }

                foreach ($messages[$stream] as $id => $fields) {
                    $metadata = [
                        'id' => $id,
                        'stream' => $stream,
                        'group' => $group,
                        'consumer' => $consumer,
                        'timestamp' => time(),
                        'type' => 'stream_group'
                    ];

                    $message = $fields['message'] ?? json_encode($fields, JSON_THROW_ON_ERROR);

                    $continue = $callback($message, $metadata);
                    if ($continue) {
                        // Acknowledge message
                        $redis->xAck($stream, $group, [$id]);
                    } else {
                        // Do not ACK — message stays in pending list for redelivery
                        $this->stop();
                        break;
                    }
                }
            } catch (RedisException | JsonException $e) {
                // @phpstan-ignore-next-line - $this->running can be changed externally
                if ($this->running) {
                    throw new ConsumerException(
                        "Failed to consume from Redis stream group: {$e->getMessage()}",
                        previous: $e
                    );
                }
            }
        }
    }

    public function getConnection(): ConnectionInterface
    {
        return $this->connection;
    }
}
