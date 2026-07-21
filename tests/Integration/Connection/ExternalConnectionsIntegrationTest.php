<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Integration\Connection;

use AMQPConnection;
use JardisAdapter\Messaging\Connection\ExternalKafkaProducerConnection;
use JardisAdapter\Messaging\Connection\ExternalRabbitMqConnection;
use JardisAdapter\Messaging\Connection\ExternalRedisConnection;
use JardisAdapter\Messaging\Publisher\KafkaPublisher;
use JardisAdapter\Messaging\Publisher\RabbitMqPublisher;
use JardisAdapter\Messaging\Publisher\RedisPublisher;
use PHPUnit\Framework\TestCase;
use RdKafka\Producer;
use Redis;

/**
 * Integration tests for External Connection wrappers with real publishers
 *
 * These tests verify that external connections work seamlessly with existing publishers
 */
class ExternalConnectionsIntegrationTest extends TestCase
{
    private static ?Redis $redis = null;
    private static ?Producer $kafkaProducer = null;
    private static ?AMQPConnection $amqpConnection = null;

    public static function setUpBeforeClass(): void
    {
        // Setup Redis
        self::$redis = new Redis();
        self::$redis->connect(
            $_ENV['REDIS_HOST'] ?? 'redis',
            (int) ($_ENV['REDIS_PORT'] ?? 6379)
        );

        // Setup Kafka
        self::$kafkaProducer = new Producer();
        self::$kafkaProducer->addBrokers($_ENV['KAFKA_BROKERS'] ?? 'kafka:9092');

        // Setup RabbitMQ
        self::$amqpConnection = new AMQPConnection([
            'host' => $_ENV['RABBITMQ_HOST'] ?? 'rabbitmq',
            'port' => (int) ($_ENV['RABBITMQ_PORT'] ?? 5672),
            'login' => $_ENV['RABBITMQ_USER'] ?? 'guest',
            'password' => $_ENV['RABBITMQ_PASSWORD'] ?? 'guest',
            'vhost' => '/',
        ]);
        self::$amqpConnection->connect();
    }

    public static function tearDownAfterClass(): void
    {
        if (self::$redis !== null) {
            self::$redis->close();
        }

        if (self::$amqpConnection !== null && self::$amqpConnection->isConnected()) {
            self::$amqpConnection->disconnect();
        }
    }

    public function testRedisPublisherWithExternalConnection(): void
    {
        $connection = new ExternalRedisConnection(self::$redis);
        $publisher = new RedisPublisher($connection, useStreams: true);

        $result = $publisher->publish('test-external-redis', 'test message');

        $this->assertTrue($result);
        $this->assertTrue($connection->isConnected());
    }

    public function testRedisPublisherWithExternalConnectionPubSub(): void
    {
        $connection = new ExternalRedisConnection(self::$redis);
        $publisher = new RedisPublisher($connection, useStreams: false);

        $result = $publisher->publish('test-external-redis-pubsub', 'test message');

        $this->assertTrue($result);
    }

    public function testKafkaPublisherWithExternalConnection(): void
    {
        $connection = new ExternalKafkaProducerConnection(self::$kafkaProducer);
        $publisher = new KafkaPublisher($connection);

        $result = $publisher->publish('test-external-kafka', 'test message');

        $this->assertTrue($result);
        $this->assertTrue($connection->isConnected());
    }

    public function testRabbitMqPublisherWithExternalConnection(): void
    {
        $connection = new ExternalRabbitMqConnection(
            self::$amqpConnection,
            exchangeName: 'test-external-exchange'
        );

        $publisher = new RabbitMqPublisher($connection);

        $result = $publisher->publish('test.routing.key', 'test message');

        $this->assertTrue($result);
        $this->assertTrue($connection->isConnected());
    }

    public function testExternalRedisConnectionLifecycleWithoutManagement(): void
    {
        $connection = new ExternalRedisConnection(self::$redis, manageLifecycle: false);
        $publisher = new RedisPublisher($connection, useStreams: true);

        $publisher->publish('test-lifecycle', 'message 1');

        // Disconnect should NOT close the external Redis connection
        $connection->disconnect();

        // Redis should still be usable directly
        $this->assertNotFalse(self::$redis->ping());
    }

    public function testExternalKafkaProducerConnectionWithFlushOnDisconnect(): void
    {
        $connection = new ExternalKafkaProducerConnection(self::$kafkaProducer, flushOnDisconnect: true);
        $publisher = new KafkaPublisher($connection);

        $publisher->publish('test-flush', 'message to flush');

        // Disconnect should flush the producer
        $connection->disconnect();

        // After disconnect, should not be connected
        $this->assertFalse($connection->isConnected());
    }

    public function testMultiplePublishesWithSameExternalConnection(): void
    {
        $connection = new ExternalRedisConnection(self::$redis);
        $publisher = new RedisPublisher($connection, useStreams: true);

        // Multiple publishes should reuse the same connection
        for ($i = 0; $i < 10; $i++) {
            $result = $publisher->publish('test-multiple', "message {$i}");
            $this->assertTrue($result);
        }

        $this->assertTrue($connection->isConnected());
    }

    public function testExternalConnectionsAreBackwardCompatible(): void
    {
        // Verify that external connections work exactly like regular connections
        $externalConnection = new ExternalRedisConnection(self::$redis);
        $publisher = new RedisPublisher($externalConnection, useStreams: true);

        $result = $publisher->publish('test-compatibility', json_encode(['test' => 'data']));

        $this->assertTrue($result);
    }
}
