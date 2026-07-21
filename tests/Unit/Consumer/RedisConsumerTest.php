<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit\Consumer;

use JardisAdapter\Messaging\Connection\RedisConnection;
use JardisAdapter\Messaging\Consumer\RedisConsumer;
use PHPUnit\Framework\TestCase;

class RedisConsumerTest extends TestCase
{
    public function testConstructorWithPubSub(): void
    {
        $connection = $this->createMock(RedisConnection::class);
        $consumer = new RedisConsumer($connection, useStreams: false);

        $this->assertInstanceOf(RedisConsumer::class, $consumer);
    }

    public function testConstructorWithStreams(): void
    {
        $connection = $this->createMock(RedisConnection::class);
        $consumer = new RedisConsumer($connection, useStreams: true);

        $this->assertInstanceOf(RedisConsumer::class, $consumer);
    }

    public function testStop(): void
    {
        $connection = $this->createMock(RedisConnection::class);
        $consumer = new RedisConsumer($connection, useStreams: true);

        $consumer->stop();

        $this->assertTrue(true); // No exception
    }
}
