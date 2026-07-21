<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Integration\Consumer;

use JardisAdapter\Messaging\Config\ConnectionConfig;
use JardisAdapter\Messaging\Consumer\KafkaConsumer;
use JardisAdapter\Messaging\Connection\KafkaConnection;
use JardisAdapter\Messaging\Connection\KafkaConsumerConnection;
use JardisAdapter\Messaging\Publisher\KafkaPublisher;
use JardisSupport\Contract\Messaging\Exception\ConsumerException;
use PHPUnit\Framework\TestCase;

class KafkaConsumerTest extends TestCase
{
    private ConnectionConfig $config;

    protected function setUp(): void
    {
        $brokers = $_ENV['KAFKA_BROKERS'] ?? 'kafka:9092';
        [$host, $port] = explode(':', $brokers);

        $this->config = new ConnectionConfig(
            host: $host,
            port: (int)$port
        );
    }

    public function testCreatesConsumer(): void
    {
        $connection = new KafkaConsumerConnection($this->config, 'test-group');
        $consumer = new KafkaConsumer($connection);

        $this->assertInstanceOf(KafkaConsumer::class, $consumer);
    }

    public function testStopsConsuming(): void
    {
        $connection = new KafkaConsumerConnection($this->config, 'test-group');
        $consumer = new KafkaConsumer($connection);

        $consumer->stop();

        $this->assertTrue(true);
    }

    public function testPublishAndConsumeMessage(): void
    {
        $uniqueSuffix = time() . '-' . rand(1000, 9999);
        $topic = 'test-topic-' . $uniqueSuffix;
        $group = 'test-group-' . $uniqueSuffix;

        // Publish messages
        $producerConnection = new KafkaConnection($this->config);
        $publisher = new KafkaPublisher($producerConnection);
        $publisher->publish($topic, json_encode(['message' => 'test1']));
        $publisher->publish($topic, json_encode(['message' => 'test2']));
        $producerConnection->disconnect();

        sleep(1);

        // Consume messages
        $consumerConnection = new KafkaConsumerConnection($this->config, $group);
        $consumer = new KafkaConsumer($consumerConnection);
        $received = [];

        $consumer->consume($topic, function (string $message, array $meta) use (&$received): bool {
            $received[] = json_decode($message, true);
            return count($received) < 2;
        }, ['timeout' => 1000, 'max_empty_polls' => 20]);

        $this->assertCount(2, $received);
        $this->assertSame('test1', $received[0]['message']);
        $this->assertSame('test2', $received[1]['message']);
    }

    public function testConsumerReceivesMetadata(): void
    {
        $uniqueSuffix = time() . '-' . rand(1000, 9999);
        $topic = 'metadata-topic-' . $uniqueSuffix;
        $group = 'metadata-group-' . $uniqueSuffix;

        $producerConnection = new KafkaConnection($this->config);
        $publisher = new KafkaPublisher($producerConnection);
        $publisher->publish($topic, 'metadata test');
        $producerConnection->disconnect();

        sleep(1);

        $consumerConnection = new KafkaConsumerConnection($this->config, $group);
        $consumer = new KafkaConsumer($consumerConnection);
        $receivedMeta = null;

        $consumer->consume($topic, function (string $message, array $meta) use (&$receivedMeta): bool {
            $receivedMeta = $meta;
            return false;
        }, ['timeout' => 1000, 'max_empty_polls' => 20]);

        $this->assertNotNull($receivedMeta);
        $this->assertArrayHasKey('topic', $receivedMeta);
        $this->assertArrayHasKey('partition', $receivedMeta);
        $this->assertArrayHasKey('offset', $receivedMeta);
        $this->assertArrayHasKey('type', $receivedMeta);
        $this->assertSame('kafka', $receivedMeta['type']);
        $this->assertSame($topic, $receivedMeta['topic']);
    }

    public function testMultipleMessagesInSequence(): void
    {
        $producerConnection = new KafkaConnection($this->config);
        $publisher = new KafkaPublisher($producerConnection);

        for ($i = 1; $i <= 5; $i++) {
            $publisher->publish('multi-test-topic', json_encode(['message' => "Message {$i}"]));
        }

        $producerConnection->disconnect();

        $this->assertTrue(true);
    }

    public function testConsumerStopsOnCallbackReturnFalse(): void
    {
        $uniqueSuffix = time() . '-' . rand(1000, 9999);
        $topic = 'stop-test-' . $uniqueSuffix;
        $group = 'stop-group-' . $uniqueSuffix;

        $producerConnection = new KafkaConnection($this->config);
        $publisher = new KafkaPublisher($producerConnection);
        $publisher->publish($topic, 'Message 1');
        $publisher->publish($topic, 'Message 2');
        $publisher->publish($topic, 'Message 3');
        $producerConnection->disconnect();

        sleep(1);

        $consumerConnection = new KafkaConsumerConnection($this->config, $group);
        $consumer = new KafkaConsumer($consumerConnection);
        $received = [];

        $consumer->consume($topic, function (string $message, array $meta) use (&$received): bool {
            $received[] = $message;
            return false;
        }, ['timeout' => 1000, 'max_empty_polls' => 20]);

        $this->assertCount(1, $received);
    }

    public function testConsumerHandlesTimeout(): void
    {
        $uniqueSuffix = time() . '-' . rand(1000, 9999);
        $topic = 'timeout-test-' . $uniqueSuffix;
        $group = 'timeout-group-' . $uniqueSuffix;

        $consumerConnection = new KafkaConsumerConnection($this->config, $group);
        $consumer = new KafkaConsumer($consumerConnection);

        $this->expectException(ConsumerException::class);

        $consumer->consume($topic, function (string $message, array $meta): bool {
            return true;
        }, ['timeout' => 100, 'max_empty_polls' => 3]);
    }

    public function testConsumerWithSaslAuth(): void
    {
        $configWithAuth = new ConnectionConfig(
            host: $this->config->host,
            port: $this->config->port,
            username: 'testuser',
            password: 'testpass'
        );

        $consumerConnection = new KafkaConsumerConnection($configWithAuth, 'auth-group');
        $consumer = new KafkaConsumer($consumerConnection);

        $this->assertInstanceOf(KafkaConsumer::class, $consumer);
    }
}
