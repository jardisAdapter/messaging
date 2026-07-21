<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Transport;

/**
 * InMemory transport for testing
 *
 * Storage for messages enabling synchronous testing without
 * external message broker infrastructure. Messages are stored per topic
 * in memory and can be consumed immediately.
 *
 * To share state between publisher and consumer, pass the same instance to both.
 */
final class InMemoryTransport
{
    /** @var array<string, array<array{message: string, timestamp: float, metadata: array<string, mixed>}>> */
    private array $queues = [];

    /**
     * Publish a message to a topic
     *
     * @param string $topic Topic name
     * @param string $message Message payload
     * @param array<string, mixed> $metadata Optional metadata
     */
    public function publish(string $topic, string $message, array $metadata = []): void
    {
        if (!isset($this->queues[$topic])) {
            $this->queues[$topic] = [];
        }

        $this->queues[$topic][] = [
            'message' => $message,
            'timestamp' => microtime(true),
            'metadata' => $metadata
        ];
    }

    /**
     * Consume all messages from a topic (removes from queue)
     *
     * @param string $topic Topic name
     * @return array<array{message: string, timestamp: float, metadata: array<string, mixed>}>
     */
    public function consume(string $topic): array
    {
        $messages = $this->queues[$topic] ?? [];
        $this->queues[$topic] = [];

        return $messages;
    }

    /**
     * Peek at messages without removing them
     *
     * @param string $topic Topic name
     * @return array<array{message: string, timestamp: float, metadata: array<string, mixed>}>
     */
    public function peek(string $topic): array
    {
        return $this->queues[$topic] ?? [];
    }

    /**
     * Get message count for a topic
     *
     * @param string $topic Topic name
     */
    public function getMessageCount(string $topic): int
    {
        return count($this->queues[$topic] ?? []);
    }

    /**
     * Clear messages
     *
     * @param string|null $topic Topic name (null = clear all topics)
     */
    public function clear(?string $topic = null): void
    {
        if ($topic === null) {
            $this->queues = [];
        } else {
            $this->queues[$topic] = [];
        }
    }

    /**
     * Re-queue a message preserving its original timestamp and metadata
     *
     * @param string $topic Topic name
     * @param array{message: string, timestamp: float, metadata: array<string, mixed>} $entry Original message entry
     */
    public function requeue(string $topic, array $entry): void
    {
        if (!isset($this->queues[$topic])) {
            $this->queues[$topic] = [];
        }

        $this->queues[$topic][] = $entry;
    }

    /**
     * Get all topic names with messages
     *
     * @return array<string>
     */
    public function getTopics(): array
    {
        return array_keys($this->queues);
    }
}
