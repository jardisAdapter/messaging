<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Publisher;

use JardisAdapter\Messaging\Connection\NullConnection;
use JardisAdapter\Messaging\Transport\InMemoryTransport;
use JardisSupport\Contract\Connection\ConnectionInterface;
use JardisSupport\Contract\Messaging\PublisherInterface;

/**
 * InMemory message publisher for testing
 *
 * Stores messages synchronously in InMemoryTransport for
 * deterministic testing without external broker infrastructure.
 */
final class InMemoryPublisher implements PublisherInterface
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
     * Publish a message to the specified topic
     *
     * @param string $topic The topic name
     * @param string $message The message payload (already serialized)
     * @param array<string, mixed> $options Options (supports 'metadata' key for custom metadata)
     * @return bool Always returns true (cannot fail in memory)
     */
    public function publish(string $topic, string $message, array $options = []): bool
    {
        $metadata = [];

        if (isset($options['metadata']) && is_array($options['metadata'])) {
            $metadata = $options['metadata'];
        }

        $this->transport->publish($topic, $message, $metadata);

        return true;
    }

    public function getConnection(): ConnectionInterface
    {
        return $this->connection;
    }
}
