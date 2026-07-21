<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Integration\Publisher;

use JardisAdapter\Messaging\Config\ConnectionConfig;
use JardisAdapter\Messaging\Connection\KafkaConnection;
use JardisAdapter\Messaging\Publisher\KafkaPublisher;
use PHPUnit\Framework\TestCase;

class KafkaPublisherTest extends TestCase
{
    private KafkaConnection $connection;
    private ConnectionConfig $config;

    protected function setUp(): void
    {
        $brokers = $_ENV['KAFKA_BROKERS'] ?? 'kafka:9092';
        [$host, $port] = explode(':', $brokers);

        $this->config = new ConnectionConfig(
            host: $host,
            port: (int)$port
        );

        $this->connection = new KafkaConnection($this->config);
    }

    protected function tearDown(): void
    {
        $this->connection->disconnect();
    }

    public function testPublishesMessage(): void
    {
        $publisher = new KafkaPublisher($this->connection);

        $result = $publisher->publish('test-topic', 'test message');

        $this->assertTrue($result);
    }

    public function testPublishesMultipleMessages(): void
    {
        $publisher = new KafkaPublisher($this->connection);

        $result1 = $publisher->publish('test-topic', 'message 1');
        $result2 = $publisher->publish('test-topic', 'message 2');
        $result3 = $publisher->publish('test-topic', 'message 3');

        $this->assertTrue($result1);
        $this->assertTrue($result2);
        $this->assertTrue($result3);
    }

    public function testPublishesToDifferentTopics(): void
    {
        $publisher = new KafkaPublisher($this->connection);

        $result1 = $publisher->publish('topic-one', 'message 1');
        $result2 = $publisher->publish('topic-two', 'message 2');

        $this->assertTrue($result1);
        $this->assertTrue($result2);
    }

    public function testPublishesWithPartition(): void
    {
        $publisher = new KafkaPublisher($this->connection);

        $result = $publisher->publish(
            'test-topic',
            'test message',
            ['partition' => 0]
        );

        $this->assertTrue($result);
    }

    public function testPublishesWithKey(): void
    {
        $publisher = new KafkaPublisher($this->connection);

        $result = $publisher->publish(
            'test-topic',
            'test message',
            ['key' => 'user-123']
        );

        $this->assertTrue($result);
    }

    public function testPublishesWithPartitionAndKey(): void
    {
        $publisher = new KafkaPublisher($this->connection);

        $result = $publisher->publish(
            'test-topic',
            'test message',
            ['partition' => 0, 'key' => 'user-123']
        );

        $this->assertTrue($result);
    }

    public function testPublishesJsonData(): void
    {
        $publisher = new KafkaPublisher($this->connection);

        $jsonMessage = json_encode(['user' => 'John', 'action' => 'login', 'timestamp' => time()]);

        $result = $publisher->publish('test-topic', $jsonMessage);

        $this->assertTrue($result);
    }

    public function testAutoConnectsOnFirstPublish(): void
    {
        $publisher = new KafkaPublisher($this->connection);

        $this->assertFalse($this->connection->isConnected());

        $result = $publisher->publish('test-topic', 'test message');

        $this->assertTrue($result);
        $this->assertTrue($this->connection->isConnected());
    }

    public function testPublishesWithExistingConnection(): void
    {
        $this->connection->connect();
        $publisher = new KafkaPublisher($this->connection);

        $result = $publisher->publish('test-topic', 'test message');

        $this->assertTrue($result);
    }

    public function testPublishesLargeMessage(): void
    {
        $publisher = new KafkaPublisher($this->connection);

        $largeMessage = str_repeat('x', 10000);

        $result = $publisher->publish('test-topic', $largeMessage);

        $this->assertTrue($result);
    }

    public function testReusesSameTopicInstance(): void
    {
        $publisher = new KafkaPublisher($this->connection);

        // Publish to same topic multiple times
        $result1 = $publisher->publish('reuse-topic', 'message 1');
        $result2 = $publisher->publish('reuse-topic', 'message 2');
        $result3 = $publisher->publish('reuse-topic', 'message 3');

        $this->assertTrue($result1);
        $this->assertTrue($result2);
        $this->assertTrue($result3);
    }

    public function testPublishesBatchOfMessages(): void
    {
        $publisher = new KafkaPublisher($this->connection);

        $messages = [];
        for ($i = 1; $i <= 100; $i++) {
            $result = $publisher->publish('batch-topic', "message {$i}");
            $messages[] = $result;
        }

        // All should succeed
        $this->assertCount(100, array_filter($messages));
    }

    public function testPublishesWithEmptyMessage(): void
    {
        $publisher = new KafkaPublisher($this->connection);

        $result = $publisher->publish('test-topic', '');

        $this->assertTrue($result);
    }

    public function testPublishesWithCustomFlushTimeout(): void
    {
        $publisher = new KafkaPublisher($this->connection);

        $result = $publisher->publish('test-topic', 'test message', ['flush_timeout_ms' => 5000]);

        $this->assertTrue($result);
    }

    public function testSwitchesBetweenTopics(): void
    {
        $publisher = new KafkaPublisher($this->connection);

        // Publish to different topics in sequence — tests getOrCreateTopic cache
        $result1 = $publisher->publish('topic-a', 'msg 1');
        $result2 = $publisher->publish('topic-b', 'msg 2');
        $result3 = $publisher->publish('topic-a', 'msg 3'); // should use cached topic

        $this->assertTrue($result1);
        $this->assertTrue($result2);
        $this->assertTrue($result3);
    }
}
