<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit;

use InvalidArgumentException;
use JardisAdapter\Messaging\Config\DatabaseTransportOptions;
use PHPUnit\Framework\TestCase;

class DatabaseTransportOptionsTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $options = new DatabaseTransportOptions();

        $this->assertSame('domain_events', $options->table);
        $this->assertSame('domain_event_subscriptions', $options->subscriptionTable);
        $this->assertFalse($options->deleteAfterProcessing);
        $this->assertSame(1000, $options->pollingIntervalMs);
        $this->assertSame(10, $options->batchSize);
        $this->assertSame(3, $options->maxAttempts);
    }

    public function testCustomValues(): void
    {
        $options = new DatabaseTransportOptions(
            table: 'custom_events',
            deleteAfterProcessing: true,
            pollingIntervalMs: 500,
            batchSize: 25,
            maxAttempts: 5,
        );

        $this->assertSame('custom_events', $options->table);
        $this->assertTrue($options->deleteAfterProcessing);
        $this->assertSame(500, $options->pollingIntervalMs);
        $this->assertSame(25, $options->batchSize);
        $this->assertSame(5, $options->maxAttempts);
    }

    public function testThrowsExceptionForInvalidTableName(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid table name');

        new DatabaseTransportOptions(table: 'DROP TABLE users;--');
    }

    public function testThrowsExceptionForTableNameStartingWithNumber(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new DatabaseTransportOptions(table: '1invalid');
    }

    public function testThrowsExceptionForEmptyTableName(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new DatabaseTransportOptions(table: '');
    }

    public function testThrowsExceptionForNegativePollingInterval(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Polling interval must be non-negative');

        new DatabaseTransportOptions(pollingIntervalMs: -1);
    }

    public function testThrowsExceptionForZeroBatchSize(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be at least 1');

        new DatabaseTransportOptions(batchSize: 0);
    }

    public function testThrowsExceptionForZeroMaxAttempts(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Max attempts must be at least 1');

        new DatabaseTransportOptions(maxAttempts: 0);
    }

    public function testAllowsUnderscoreInTableName(): void
    {
        $options = new DatabaseTransportOptions(table: '_my_table_name');
        $this->assertSame('_my_table_name', $options->table);
    }

    public function testAllowsZeroPollingInterval(): void
    {
        $options = new DatabaseTransportOptions(pollingIntervalMs: 0);
        $this->assertSame(0, $options->pollingIntervalMs);
    }

    public function testCustomSubscriptionTable(): void
    {
        $options = new DatabaseTransportOptions(subscriptionTable: 'custom_subscriptions');
        $this->assertSame('custom_subscriptions', $options->subscriptionTable);
    }

    public function testThrowsExceptionForInvalidSubscriptionTableName(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid subscription table name');

        new DatabaseTransportOptions(subscriptionTable: 'DROP TABLE;--');
    }
}
