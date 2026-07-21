<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging;

use JardisSupport\Contract\Messaging\ConsumerInterface;
use JardisSupport\Contract\Messaging\MessageConsumerInterface;
use JardisSupport\Contract\Messaging\MessageHandlerInterface;
use JardisSupport\Contract\Messaging\Exception\ConsumerException;
use JardisSupport\Contract\Messaging\Exception\MessageException;
use JsonException;

/**
 * Immutable message consumer facade with layered fallback support
 *
 * Consumers are injected via constructor in priority order (first = highest priority).
 * On failure (MessageException), the next consumer is tried automatically.
 * Graceful shutdown (SIGTERM/SIGINT) is enabled automatically.
 *
 * Usage:
 * $consumer = new MessageConsumer(
 *     $conFactory->redis($conn),
 * );
 * $consumer->consume('topic', $handler);
 */
class MessageConsumer implements MessageConsumerInterface
{
    private bool $gracefulShutdownEnabled = false;

    /** @var array<int, ConsumerInterface> */
    private readonly array $consumers;

    public function __construct(ConsumerInterface ...$consumers)
    {
        $this->consumers = array_values($consumers);
    }

    /**
     * Start consuming messages with a handler
     *
     * Tries each configured consumer in order (fallback on failure)
     * Automatically enables graceful shutdown if not already enabled
     *
     * @param string $topic The topic, channel or queue name
     * @param MessageHandlerInterface $handler Message handler
     * @param array<string, mixed> $options Consumer-specific options
     * @throws ConsumerException if no consumers configured or all fail
     */
    public function consume(string $topic, MessageHandlerInterface $handler, array $options = []): void
    {
        if (empty($this->consumers)) {
            throw new ConsumerException(
                'No consumers configured. Pass ConsumerInterface instances to the constructor.'
            );
        }

        if (!$this->gracefulShutdownEnabled) {
            $this->enableGracefulShutdown();
        }

        $callback = function (string $rawMessage, array $metadata) use ($handler): bool {
            $message = $this->deserialize($rawMessage);
            return $handler->handle($message, $metadata);
        };

        $errors = [];

        foreach ($this->consumers as $consumer) {
            try {
                $consumer->consume($topic, $callback, $options);
                return;
            } catch (MessageException $e) {
                $errors[] = $this->resolveLabel($consumer) . ': ' . $e->getMessage();
            }
        }

        throw new ConsumerException(
            'All consumer layers failed. Errors: ' . implode(' | ', $errors)
        );
    }

    /**
     * Stop consuming messages
     */
    public function stop(): void
    {
        foreach ($this->consumers as $consumer) {
            $consumer->stop();
        }
    }

    /**
     * Try to deserialize JSON, fallback to raw string
     *
     * @param string $raw Raw message
     * @return string|array<mixed> Deserialized message or raw string
     */
    private function deserialize(string $raw): string|array
    {
        if ($raw === '') {
            return $raw;
        }

        try {
            $decoded = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
            return is_array($decoded) ? $decoded : $raw;
        } catch (JsonException) {
            return $raw;
        }
    }

    /**
     * Enable graceful shutdown via system signals (SIGTERM, SIGINT)
     */
    private function enableGracefulShutdown(): void
    {
        if (function_exists('pcntl_async_signals') && function_exists('pcntl_signal')) {
            pcntl_async_signals(true);
            pcntl_signal(SIGTERM, fn() => $this->stop());
            pcntl_signal(SIGINT, fn() => $this->stop());
            $this->gracefulShutdownEnabled = true;
        }
    }

    /**
     * Resolve a human-readable label from the consumer class name
     */
    private function resolveLabel(ConsumerInterface $consumer): string
    {
        $class = get_class($consumer);
        $shortName = substr($class, (int) strrpos($class, '\\') + 1);

        return str_replace('Consumer', '', $shortName);
    }
}
