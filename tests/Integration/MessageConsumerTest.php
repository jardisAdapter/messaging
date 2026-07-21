<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Integration;

use JardisAdapter\Messaging\Config\ConnectionConfig;
use JardisAdapter\Messaging\Connection\RedisConnection;
use JardisAdapter\Messaging\Consumer\RedisConsumer;
use JardisAdapter\Messaging\Factory\ConnectionFactory;
use JardisAdapter\Messaging\Factory\ConsumerFactory;
use JardisAdapter\Messaging\Handler\CallbackHandler;
use JardisAdapter\Messaging\MessageConsumer;
use JardisAdapter\Messaging\Publisher\RedisPublisher;
use PHPUnit\Framework\TestCase;

class MessageConsumerTest extends TestCase
{
    private ConnectionConfig $config;
    private RedisConnection $connection;
    private ConnectionFactory $connectionFactory;
    private ConsumerFactory $factory;

    protected function setUp(): void
    {
        $this->config = new ConnectionConfig(
            host: $_ENV['REDIS_HOST'] ?? 'redis',
            port: 6379
        );

        $this->connection = new RedisConnection($this->config);
        $this->connectionFactory = new ConnectionFactory();
        $this->factory = new ConsumerFactory();
    }

    protected function tearDown(): void
    {
        $this->connection->disconnect();
    }

    public function testCreatesMessageConsumer(): void
    {
        $redisConsumer = new RedisConsumer($this->connection, useStreams: true);
        $messageConsumer = new MessageConsumer($redisConsumer);

        $this->assertInstanceOf(MessageConsumer::class, $messageConsumer);
    }

    public function testStopsConsuming(): void
    {
        $redisConsumer = new RedisConsumer($this->connection, useStreams: true);
        $messageConsumer = new MessageConsumer($redisConsumer);

        $messageConsumer->stop();

        $this->assertTrue(true);
    }

    public function testAutoDeserializesJsonMessage(): void
    {
        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $publisher->publish('json.auto.stream', json_encode(['user' => 'John', 'action' => 'login']));

        $redisConsumer = new RedisConsumer($this->connection, useStreams: true);
        $messageConsumer = new MessageConsumer($redisConsumer);

        $receivedMessage = null;
        $handler = new CallbackHandler(function (string|array $msg, array $meta) use (&$receivedMessage): bool {
            $receivedMessage = $msg;
            return false;
        });

        $messageConsumer->consume('json.auto.stream', $handler, ['start_id' => '0', 'block' => 100]);

        $this->assertIsArray($receivedMessage);
        $this->assertSame('John', $receivedMessage['user']);
        $this->assertSame('login', $receivedMessage['action']);

        $this->connection->connect();
        $this->connection->getClient()->del('json.auto.stream');
    }

    public function testHandlerReceivesMetadata(): void
    {
        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $publisher->publish('meta.handler.stream', 'test message');

        $receivedMeta = null;
        $handler = new CallbackHandler(function (string|array $msg, array $meta) use (&$receivedMeta): bool {
            $receivedMeta = $meta;
            return false;
        });

        $redisConsumer = new RedisConsumer($this->connection, useStreams: true);
        $messageConsumer = new MessageConsumer($redisConsumer);

        $messageConsumer->consume('meta.handler.stream', $handler, ['start_id' => '0', 'block' => 100]);

        $this->assertNotNull($receivedMeta);
        $this->assertArrayHasKey('id', $receivedMeta);
        $this->assertArrayHasKey('stream', $receivedMeta);
        $this->assertSame('meta.handler.stream', $receivedMeta['stream']);

        $this->connection->connect();
        $this->connection->getClient()->del('meta.handler.stream');
    }

    public function testDeserializesInvalidJsonAsString(): void
    {
        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $publisher->publish('plain.invalid.stream', 'plain text message');

        $redisConsumer = new RedisConsumer($this->connection, useStreams: true);
        $messageConsumer = new MessageConsumer($redisConsumer);

        $receivedMessage = null;
        $handler = new CallbackHandler(function (string|array $msg, array $meta) use (&$receivedMessage): bool {
            $receivedMessage = $msg;
            return false;
        });

        $messageConsumer->consume('plain.invalid.stream', $handler, ['start_id' => '0', 'block' => 100]);

        $this->assertIsString($receivedMessage);
        $this->assertSame('plain text message', $receivedMessage);

        $this->connection->connect();
        $this->connection->getClient()->del('plain.invalid.stream');
    }

    public function testPublishAndVerifyStreamExists(): void
    {
        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $publisher->publish('verify.stream', json_encode(['test' => 'data']));

        $this->connection->connect();
        $redis = $this->connection->getClient();

        $messages = $redis->xRange('verify.stream', '-', '+');
        $this->assertNotEmpty($messages);

        $firstMessage = reset($messages);
        $decoded = json_decode($firstMessage['message'], true);
        $this->assertSame('data', $decoded['test']);

        $redis->del('verify.stream');
    }

    public function testFluentApiWithRedisFactory(): void
    {
        $redisConn = $this->connectionFactory->redis($_ENV['REDIS_HOST'] ?? 'redis', 6379);

        $consumer = new MessageConsumer($this->factory->redis($redisConn));

        $this->assertInstanceOf(MessageConsumer::class, $consumer);
    }

    public function testFluentApiWithKafkaFactory(): void
    {
        $brokers = $_ENV['KAFKA_BROKERS'] ?? 'kafka:9092';
        $kafkaConn = $this->connectionFactory->kafkaConsumer($brokers, 'test-consumer-group');

        $consumer = new MessageConsumer($this->factory->kafka($kafkaConn));

        $this->assertInstanceOf(MessageConsumer::class, $consumer);
    }

    public function testFluentApiWithRabbitMqFactory(): void
    {
        $rabbitConn = $this->connectionFactory->rabbitMq(
            $_ENV['RABBITMQ_HOST'] ?? 'rabbitmq',
            (int)($_ENV['RABBITMQ_PORT'] ?? 5672),
            $_ENV['RABBITMQ_USER'] ?? 'guest',
            $_ENV['RABBITMQ_PASSWORD'] ?? 'guest'
        );

        $consumer = new MessageConsumer(
            $this->factory->rabbitMq($rabbitConn, 'test_fluent_queue'),
        );

        $this->assertInstanceOf(MessageConsumer::class, $consumer);
    }

    public function testFluentApiMultipleLayersWithAllBrokers(): void
    {
        $brokers = $_ENV['KAFKA_BROKERS'] ?? 'kafka:9092';
        $redisConn = $this->connectionFactory->redis($_ENV['REDIS_HOST'] ?? 'redis', 6379);
        $kafkaConn = $this->connectionFactory->kafkaConsumer($brokers, 'multi-layer-group');
        $rabbitConn = $this->connectionFactory->rabbitMq(
            $_ENV['RABBITMQ_HOST'] ?? 'rabbitmq',
            (int)($_ENV['RABBITMQ_PORT'] ?? 5672),
            $_ENV['RABBITMQ_USER'] ?? 'guest',
            $_ENV['RABBITMQ_PASSWORD'] ?? 'guest'
        );

        $consumer = new MessageConsumer(
            $this->factory->redis($redisConn),
            $this->factory->kafka($kafkaConn),
            $this->factory->rabbitMq($rabbitConn, 'multi_layer_queue'),
        );

        $this->assertInstanceOf(MessageConsumer::class, $consumer);
    }

    public function testThrowsExceptionWhenNoConsumersConfigured(): void
    {
        $consumer = new MessageConsumer();
        $handler = new CallbackHandler(fn($m, $meta) => false);

        $this->expectException(\JardisSupport\Contract\Messaging\Exception\ConsumerException::class);
        $this->expectExceptionMessage('No consumers configured');

        $consumer->consume('test', $handler);
    }

    public function testStopAllConsumers(): void
    {
        $redisConn = $this->connectionFactory->redis($_ENV['REDIS_HOST'] ?? 'redis', 6379);

        $consumer = new MessageConsumer($this->factory->redis($redisConn));

        $consumer->stop();

        $this->assertTrue(true);
    }

    public function testDeserializeEmptyString(): void
    {
        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $publisher->publish('empty.stream', '');

        $redisConsumer = new RedisConsumer($this->connection, useStreams: true);
        $messageConsumer = new MessageConsumer($redisConsumer);

        $receivedMessage = null;
        $handler = new CallbackHandler(function (string|array $msg, array $meta) use (&$receivedMessage): bool {
            $receivedMessage = $msg;
            return false;
        });

        $messageConsumer->consume('empty.stream', $handler, ['start_id' => '0', 'block' => 100]);

        $this->assertIsString($receivedMessage);
        $this->assertSame('', $receivedMessage);

        $this->connection->connect();
        $this->connection->getClient()->del('empty.stream');
    }
}
