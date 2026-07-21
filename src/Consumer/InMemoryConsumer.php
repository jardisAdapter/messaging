<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Consumer;

use JardisAdapter\Messaging\Connection\NullConnection;
use JardisAdapter\Messaging\Transport\InMemoryTransport;
use JardisSupport\Contract\Connection\ConnectionInterface;
use JardisSupport\Contract\Messaging\ConsumerInterface;

/**
 * InMemory message consumer for testing
 *
 * Processes messages synchronously from InMemoryTransport for
 * deterministic testing without external broker infrastructure.
 * Unlike async consumers, this processes all messages immediately
 * and returns (no blocking loop).
 */
final class InMemoryConsumer implements ConsumerInterface
{
    private readonly InMemoryTransport $transport;
    private readonly NullConnection $connection;

    /**
     * @param InMemoryTransport|null $transport Optional shared transport instance (creates new if null)
     */
    public function __construct(?InMemoryTransport $transport = null)
    {
        $this->transport = $transport ?? new InMemoryTransport();
        $this->connection = new NullConnection();
    }

    /**
     * Consume messages from the specified topic (synchronously)
     *
     * Processes all available messages immediately and returns.
     * Does NOT run indefinitely like async consumers.
     *
     * @param string $topic The topic name
     * @param callable $callback Message handler: function(string $message, array $metadata): bool
     * @param array<string, mixed> $options Options:
     *                                       - 'limit': Max messages to process (default: all)
     *                                       - 'peek': If true, don't remove messages from queue
     */
    public function consume(string $topic, callable $callback, array $options = []): void
    {
        $limit = isset($options['limit']) && is_int($options['limit']) ? $options['limit'] : null;
        $peek = isset($options['peek']) && $options['peek'] === true;

        $messages = $peek
            ? $this->transport->peek($topic)
            : $this->transport->consume($topic);

        $processed = 0;

        foreach ($messages as $index => $entry) {
            if ($limit !== null && $processed >= $limit) {
                // Re-queue remaining messages if we hit the limit
                if (!$peek) {
                    $remaining = array_slice($messages, $processed);
                    foreach ($remaining as $remainingEntry) {
                        $this->transport->requeue($topic, $remainingEntry);
                    }
                }
                break;
            }

            $metadata = array_merge([
                'topic' => $topic,
                'timestamp' => $entry['timestamp'],
                'type' => 'inmemory',
                'index' => $index
            ], $entry['metadata']);

            try {
                $continue = $callback($entry['message'], $metadata);
            } catch (\Throwable $e) {
                if (!$peek) {
                    $remaining = array_slice($messages, $processed);
                    foreach ($remaining as $remainingEntry) {
                        $this->transport->requeue($topic, $remainingEntry);
                    }
                }
                throw $e;
            }

            if (!$continue) {
                // Re-queue current and remaining unprocessed messages
                if (!$peek) {
                    $remaining = array_slice($messages, $processed);
                    foreach ($remaining as $remainingEntry) {
                        $this->transport->requeue($topic, $remainingEntry);
                    }
                }
                break;
            }

            $processed++;
        }
    }

    /**
     * Stop consuming messages
     *
     * No-op for sync consumer (there's no loop to stop)
     */
    public function stop(): void
    {
        // No-op: InMemory consumer is synchronous, no loop to stop
    }

    public function getConnection(): ConnectionInterface
    {
        return $this->connection;
    }
}
