<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Validation;

use Closure;
use DateTimeInterface;
use JardisSupport\Contract\Messaging\Exception\PublishException;
use JsonSerializable;
use Stringable;

/**
 * Validates message payloads before serialization
 */
class MessageValidator
{
    /**
     * Validate that array contains only serializable values
     *
     * @param array<mixed> $data
     * @throws PublishException If array contains non-serializable values
     */
    public function validate(array $data): void
    {
        array_walk_recursive($data, function ($value, $key): void {
            if (is_resource($value)) {
                throw new PublishException("Cannot serialize resource at key '{$key}'");
            }

            if ($value instanceof Closure) {
                throw new PublishException("Cannot serialize closure at key '{$key}'");
            }

            // Allow objects that implement JsonSerializable or Stringable
            if (
                is_object($value)
                && !$value instanceof JsonSerializable
                && !$value instanceof Stringable
                && !$value instanceof DateTimeInterface
            ) {
                $class = get_class($value);
                throw new PublishException(
                    "Object of type '{$class}' at key '{$key}' is not JSON serializable. " .
                    "Implement JsonSerializable or convert to array first."
                );
            }
        });
    }
}
