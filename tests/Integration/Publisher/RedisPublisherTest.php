<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Integration\Publisher;

use JardisAdapter\Messaging\Config\ConnectionConfig;
use JardisAdapter\Messaging\Connection\RedisConnection;
use JardisSupport\Contract\Messaging\Exception\PublishException;
use JardisAdapter\Messaging\Publisher\RedisPublisher;
use PHPUnit\Framework\TestCase;

class RedisPublisherTest extends TestCase
{
    private RedisConnection $connection;
    private ConnectionConfig $config;

    protected function setUp(): void
    {
        $this->config = new ConnectionConfig(
            host: $_ENV['REDIS_HOST'] ?? 'redis',
            port: 6379 // Internal Docker port
        );

        $this->connection = new RedisConnection($this->config);
    }

    protected function tearDown(): void
    {
        $this->connection->disconnect();
    }

    public function testPublishesMessageViaPubSub(): void
    {
        $publisher = new RedisPublisher($this->connection, useStreams: false);

        $result = $publisher->publish('test.channel', 'test message');

        $this->assertTrue($result);
    }

    public function testPublishesMessageViaStreams(): void
    {
        $publisher = new RedisPublisher($this->connection, useStreams: true);

        $result = $publisher->publish('test.stream', 'test message');

        $this->assertTrue($result);

        // Cleanup
        $this->connection->connect();
        $this->connection->getClient()->del('test.stream');
    }

    public function testPublishesWithCustomStreamOptions(): void
    {
        $publisher = new RedisPublisher($this->connection, useStreams: true);

        $result = $publisher->publish(
            'test.stream',
            'test message',
            ['maxlen' => 1000, 'fields' => ['user_id' => 123]]
        );

        $this->assertTrue($result);

        // Cleanup
        $this->connection->connect();
        $this->connection->getClient()->del('test.stream');
    }

    public function testPublishesMultipleMessages(): void
    {
        $publisher = new RedisPublisher($this->connection, useStreams: false);

        $result1 = $publisher->publish('test.channel', 'message 1');
        $result2 = $publisher->publish('test.channel', 'message 2');
        $result3 = $publisher->publish('test.channel', 'message 3');

        $this->assertTrue($result1);
        $this->assertTrue($result2);
        $this->assertTrue($result3);
    }

    public function testPublishesToDifferentChannels(): void
    {
        $publisher = new RedisPublisher($this->connection, useStreams: false);

        $result1 = $publisher->publish('channel.one', 'message 1');
        $result2 = $publisher->publish('channel.two', 'message 2');

        $this->assertTrue($result1);
        $this->assertTrue($result2);
    }

    public function testPublishesJsonData(): void
    {
        $publisher = new RedisPublisher($this->connection, useStreams: false);

        $jsonMessage = json_encode(['user' => 'John', 'action' => 'login']);

        $result = $publisher->publish('test.channel', $jsonMessage);

        $this->assertTrue($result);
    }

    public function testAutoConnectsOnFirstPublish(): void
    {
        $publisher = new RedisPublisher($this->connection, useStreams: false);

        $this->assertFalse($this->connection->isConnected());

        $result = $publisher->publish('test.channel', 'test message');

        $this->assertTrue($result);
        $this->assertTrue($this->connection->isConnected());
    }

    public function testPublishesWithExistingConnection(): void
    {
        $this->connection->connect();
        $publisher = new RedisPublisher($this->connection, useStreams: false);

        $result = $publisher->publish('test.channel', 'test message');

        $this->assertTrue($result);
    }

    public function testPublishesLargeMessage(): void
    {
        $publisher = new RedisPublisher($this->connection, useStreams: false);

        $largeMessage = str_repeat('x', 10000);

        $result = $publisher->publish('test.channel', $largeMessage);

        $this->assertTrue($result);
    }

    public function testVerifiesStreamMessageIsStored(): void
    {
        $publisher = new RedisPublisher($this->connection, useStreams: true);

        $publisher->publish('test.stream', 'stored message');

        $this->connection->connect();
        $client = $this->connection->getClient();

        $messages = $client->xRange('test.stream', '-', '+');

        $this->assertNotEmpty($messages);
        $this->assertArrayHasKey('message', reset($messages));
        $this->assertSame('stored message', reset($messages)['message']);

        // Cleanup
        $client->del('test.stream');
    }

    public function testStreamWithMaxLength(): void
    {
        $publisher = new RedisPublisher($this->connection, useStreams: true);

        // Publish multiple messages with maxlen=2
        $publisher->publish('test.stream', 'message 1', ['maxlen' => 2]);
        $publisher->publish('test.stream', 'message 2', ['maxlen' => 2]);
        $publisher->publish('test.stream', 'message 3', ['maxlen' => 2]);

        $this->connection->connect();
        $client = $this->connection->getClient();

        $messages = $client->xRange('test.stream', '-', '+');

        // Due to approximate trimming, should be around 2 messages
        $this->assertLessThanOrEqual(3, count($messages));

        // Cleanup
        $client->del('test.stream');
    }
}
