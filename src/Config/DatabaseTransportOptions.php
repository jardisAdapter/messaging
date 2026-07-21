<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Config;

use InvalidArgumentException;

/**
 * Configuration for the database transport
 *
 * Controls table name, cleanup behavior, polling, and retry settings
 * for DatabasePublisher and DatabaseConsumer.
 */
readonly class DatabaseTransportOptions
{
    /**
     * @param string $table Database table name for events (alphanumeric and underscores only)
     * @param string $subscriptionTable Table name for fan-out subscriptions (alphanumeric and underscores only)
     * @param bool $deleteAfterProcessing If true, deletes events after successful processing (hard delete).
     *                                     If false, sets processed_at timestamp (soft delete, default).
     * @param int $pollingIntervalMs Milliseconds to wait when no messages are available
     * @param int $batchSize Maximum number of messages to fetch per poll cycle
     * @param int $maxAttempts Maximum processing attempts before skipping a message
     * @throws InvalidArgumentException If table name contains invalid characters
     */
    public function __construct(
        public string $table = 'domain_events',
        public string $subscriptionTable = 'domain_event_subscriptions',
        public bool $deleteAfterProcessing = false,
        public int $pollingIntervalMs = 1000,
        public int $batchSize = 10,
        public int $maxAttempts = 3,
    ) {
        if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $table)) {
            throw new InvalidArgumentException(
                "Invalid table name '{$table}': only alphanumeric characters and underscores allowed"
            );
        }

        if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $subscriptionTable)) {
            throw new InvalidArgumentException(
                "Invalid subscription table name '{$subscriptionTable}': "
                . "only alphanumeric characters and underscores allowed"
            );
        }

        if ($pollingIntervalMs < 0) {
            throw new InvalidArgumentException('Polling interval must be non-negative');
        }

        if ($batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be at least 1');
        }

        if ($maxAttempts < 1) {
            throw new InvalidArgumentException('Max attempts must be at least 1');
        }
    }
}
