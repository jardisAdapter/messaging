<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Connection;

use JardisSupport\Contract\Connection\ConnectionInterface;

/**
 * Null Object for InMemory publisher/consumer
 */
class NullConnection implements ConnectionInterface
{
    public function connect(): void
    {
    }

    public function disconnect(): void
    {
    }

    public function isConnected(): bool
    {
        return true;
    }
}
