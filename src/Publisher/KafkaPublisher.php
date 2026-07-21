<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Publisher;

use JardisAdapter\Messaging\Connection\KafkaProducerConnectionInterface;
use JardisSupport\Contract\Connection\ConnectionInterface;
use JardisSupport\Contract\Messaging\PublisherInterface;
use JardisSupport\Contract\Messaging\Exception\PublishException;
use RdKafka\ProducerTopic;
use Exception;

/**
 * Kafka message publisher
 *
 * Uses Kafka producer for message publishing
 */
class KafkaPublisher implements PublisherInterface
{
    /** @var array<string, ProducerTopic> */
    private array $topics = [];

    private ?object $lastProducer = null;

    public function __construct(
        private readonly KafkaProducerConnectionInterface $connection
    ) {
    }

    /**
     * Publish a message to the specified topic
     *
     * @param string $topic The Kafka topic name
     * @param string $message The message payload (already serialized)
     * @param array<string, mixed> $options Publisher-specific options (partition, key, flush_timeout_ms)
     * @return bool True on success
     * @throws PublishException
     */
    public function publish(string $topic, string $message, array $options = []): bool
    {
        if (!$this->connection->isConnected()) {
            $this->connection->connect();
        }

        try {
            $producerTopic = $this->getOrCreateTopic($topic);

            $partition = $options['partition'] ?? RD_KAFKA_PARTITION_UA;
            $key = $options['key'] ?? null;

            $producerTopic->produce($partition, 0, $message, $key);

            $producer = $this->connection->getClient();
            $flushTimeoutMs = $options['flush_timeout_ms'] ?? 1000;
            $result = $producer->flush($flushTimeoutMs);

            if ($result !== RD_KAFKA_RESP_ERR_NO_ERROR) {
                throw new PublishException(
                    "Kafka flush timed out for topic '{$topic}' — message may not have been delivered"
                );
            }

            return true;
        } catch (PublishException $e) {
            throw $e;
        } catch (Exception $e) {
            throw new PublishException(
                "Failed to publish message to Kafka topic '{$topic}': {$e->getMessage()}",
                previous: $e
            );
        }
    }

    /**
     * Get or create a producer topic
     */
    private function getOrCreateTopic(string $topicName): ProducerTopic
    {
        $producer = $this->connection->getClient();

        if ($this->lastProducer !== $producer) {
            $this->topics = [];
            $this->lastProducer = $producer;
        }

        if (!isset($this->topics[$topicName])) {
            $this->topics[$topicName] = $producer->newTopic($topicName);
        }

        return $this->topics[$topicName];
    }

    public function getConnection(): ConnectionInterface
    {
        return $this->connection;
    }
}
