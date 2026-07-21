<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Consumer;

use JardisAdapter\Messaging\Connection\KafkaConsumerConnectionInterface;
use JardisSupport\Contract\Connection\ConnectionInterface;
use JardisSupport\Contract\Messaging\ConsumerInterface;
use JardisSupport\Contract\Messaging\Exception\ConsumerException;
use RdKafka\KafkaConsumer as RdKafkaConsumer;
use RdKafka\Message;

/**
 * Kafka message consumer
 *
 * Supports both internal and external Kafka consumer instances.
 * Uses Kafka consumer groups for scalable message consumption.
 */
class KafkaConsumer implements ConsumerInterface
{
    private bool $running = false;

    public function __construct(
        private readonly KafkaConsumerConnectionInterface $connection,
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

        $consumer = $this->connection->getClient();
        $this->running = true;
        $consumer->subscribe([$topic]);

        $timeoutMs = $options['timeout'] ?? 1000;
        $maxEmptyPolls = $options['max_empty_polls'] ?? 0;
        $emptyPollCount = 0;

        while ($this->running) {
            $message = $consumer->consume($timeoutMs);

            $hadMessage = $this->handleMessage($message, $consumer, $callback);

            if (!$hadMessage) {
                $emptyPollCount++;
                if ($maxEmptyPolls > 0 && $emptyPollCount >= $maxEmptyPolls) {
                    break;
                }
            } else {
                $emptyPollCount = 0;
            }
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
     * Handle consumed message
     *
     * @param Message $message Kafka message
     * @param RdKafkaConsumer $consumer Kafka consumer for committing
     * @param callable $callback Message handler
     * @return bool True if a message was processed, false otherwise
     */
    private function handleMessage(Message $message, RdKafkaConsumer $consumer, callable $callback): bool
    {
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                $metadata = [
                    'partition' => $message->partition,
                    'offset' => $message->offset,
                    'timestamp' => $message->timestamp,
                    'key' => $message->key,
                    'topic' => $message->topic_name,
                    'type' => 'kafka'
                ];

                try {
                    $continue = $callback($message->payload, $metadata);
                } catch (\Throwable $e) {
                    $this->stop();
                    throw $e;
                }
                if ($continue) {
                    $consumer->commit($message);
                } else {
                    $this->stop();
                }
                return true;

            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                return false;

            default:
                if ($this->running) {
                    throw new ConsumerException(
                        "Kafka consumer error: {$message->errstr()} (code: {$message->err})"
                    );
                }
                return false;
        }
    }

    public function getConnection(): ConnectionInterface
    {
        return $this->connection;
    }
}
