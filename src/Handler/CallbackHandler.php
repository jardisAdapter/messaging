<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Handler;

use Closure;
use JardisSupport\Contract\Messaging\MessageHandlerInterface;

/**
 * Callback-based message handler
 *
 * Wraps a closure/callable as a MessageHandlerInterface implementation
 */
class CallbackHandler implements MessageHandlerInterface
{
    /**
     * @param Closure $callback Callback function(string|array $message, array $metadata): bool
     */
    public function __construct(
        private readonly Closure $callback
    ) {
    }

    /**
     * @inheritDoc
     */
    public function handle(string|array $message, array $metadata): bool
    {
        return ($this->callback)($message, $metadata);
    }
}
