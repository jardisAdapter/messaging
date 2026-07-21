<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging;

use JardisSupport\Contract\Messaging\MessagePublisherInterface;
use JardisSupport\Contract\Messaging\PublisherInterface;
use JardisSupport\Contract\Messaging\Exception\PublishException;
use JardisAdapter\Messaging\Validation\MessageValidator;
use JardisSupport\Contract\Messaging\Exception\MessageException;

/**
 * Immutable message publisher facade with layered fallback support
 *
 * Publishers are injected via constructor in priority order (first = highest priority).
 * On failure (MessageException), the next publisher is tried automatically.
 *
 * Usage:
 * $publisher = new MessagePublisher(
 *     $pubFactory->redis($primaryConn),
 *     $pubFactory->redis($fallbackConn),
 * );
 * $publisher->publish('topic', $message);
 */
class MessagePublisher implements MessagePublisherInterface
{
    private MessageValidator $validator;

    /** @var array<int, PublisherInterface> */
    private readonly array $publishers;

    public function __construct(PublisherInterface ...$publishers)
    {
        $this->publishers = array_values($publishers);
        $this->validator  = new MessageValidator();
    }

    /**
     * Return a new instance with the given validator injected.
     *
     * Usage: $publisher = (new MessagePublisher($pub1, $pub2))->withValidator($validator);
     */
    public function withValidator(MessageValidator $validator): static
    {
        $clone            = clone $this;
        $clone->validator = $validator;

        return $clone;
    }

    /**
     * Publish a message to the specified topic/channel/queue
     *
     * Tries each configured publisher in order (fallback on failure)
     *
     * @param string $topic The topic, channel or queue name
     * @param string|object|array<mixed> $message The message payload (strings passed as-is, arrays encoded to JSON)
     * @param array<string, mixed> $options Publisher-specific options
     * @return bool True on success
     * @throws PublishException if no publishers configured or all fail
     */
    public function publish(string $topic, string|object|array $message, array $options = []): bool
    {
        if (empty($this->publishers)) {
            throw new PublishException(
                'No publishers configured. Pass PublisherInterface instances to the constructor.'
            );
        }

        $errors = [];
        $serialized = $this->serialize($message);

        foreach ($this->publishers as $publisher) {
            try {
                return $publisher->publish($topic, $serialized, $options);
            } catch (MessageException $e) {
                $errors[] = $this->resolveLabel($publisher) . ': ' . $e->getMessage();
            }
        }

        throw new PublishException(
            'All publisher layers failed. Errors: ' . implode(' | ', $errors)
        );
    }

    /**
     * Serialize message for transmission
     *
     * @param string|object|array<mixed> $message
     * @throws PublishException
     */
    private function serialize(string|object|array $message): string
    {
        if (is_string($message)) {
            return $message;
        }

        if (is_object($message)) {
            return json_encode($message, JSON_THROW_ON_ERROR);
        }

        $this->validator->validate($message);

        return json_encode($message, JSON_THROW_ON_ERROR);
    }

    /**
     * Resolve a human-readable label from the publisher class name
     */
    private function resolveLabel(PublisherInterface $publisher): string
    {
        $class = get_class($publisher);
        $shortName = substr($class, (int) strrpos($class, '\\') + 1);

        return str_replace('Publisher', '', $shortName);
    }
}
