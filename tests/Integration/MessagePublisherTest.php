<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Integration;

use JardisAdapter\Messaging\Config\ConnectionConfig;
use JardisAdapter\Messaging\Connection\KafkaConnection;
use JardisAdapter\Messaging\Connection\RabbitMqConnection;
use JardisAdapter\Messaging\Connection\RedisConnection;
use JardisAdapter\Messaging\Factory\ConnectionFactory;
use JardisAdapter\Messaging\Factory\PublisherFactory;
use JardisSupport\Contract\Messaging\Exception\PublishException;
use JardisAdapter\Messaging\MessagePublisher;
use JardisAdapter\Messaging\Publisher\KafkaPublisher;
use JardisAdapter\Messaging\Publisher\RabbitMqPublisher;
use JardisAdapter\Messaging\Publisher\RedisPublisher;
use PHPUnit\Framework\TestCase;

class MessagePublisherTest extends TestCase
{
    private ConnectionFactory $connectionFactory;
    private PublisherFactory $factory;

    protected function setUp(): void
    {
        $this->connectionFactory = new ConnectionFactory();
        $this->factory = new PublisherFactory();
    }

    public function testPublishesStringMessageWithRedis(): void
    {
        $config = new ConnectionConfig(
            host: $_ENV['REDIS_HOST'] ?? 'redis',
            port: 6379
        );

        $connection = new RedisConnection($config);
        $publisher = new RedisPublisher($connection);
        $messagePublisher = new MessagePublisher($publisher);

        $result = $messagePublisher->publish('test.channel', 'Hello World');

        $this->assertTrue($result);

        $connection->disconnect();
    }

    public function testPublishesArrayMessageWithRedis(): void
    {
        $config = new ConnectionConfig(
            host: $_ENV['REDIS_HOST'] ?? 'redis',
            port: 6379
        );

        $connection = new RedisConnection($config);
        $publisher = new RedisPublisher($connection);
        $messagePublisher = new MessagePublisher($publisher);

        $result = $messagePublisher->publish('test.channel', [
            'user' => 'John',
            'action' => 'login',
            'timestamp' => time()
        ]);

        $this->assertTrue($result);

        $connection->disconnect();
    }

    public function testPublishesObjectMessageWithRedis(): void
    {
        $config = new ConnectionConfig(
            host: $_ENV['REDIS_HOST'] ?? 'redis',
            port: 6379
        );

        $connection = new RedisConnection($config);
        $publisher = new RedisPublisher($connection);
        $messagePublisher = new MessagePublisher($publisher);

        $object = new class implements \JsonSerializable {
            public function jsonSerialize(): array
            {
                return ['type' => 'test', 'value' => 123];
            }
        };

        $result = $messagePublisher->publish('test.channel', $object);

        $this->assertTrue($result);

        $connection->disconnect();
    }

    public function testPublishesStringMessageWithKafka(): void
    {
        $brokers = $_ENV['KAFKA_BROKERS'] ?? 'kafka:9092';
        [$host, $port] = explode(':', $brokers);

        $config = new ConnectionConfig(host: $host, port: (int)$port);
        $connection = new KafkaConnection($config);
        $publisher = new KafkaPublisher($connection);
        $messagePublisher = new MessagePublisher($publisher);

        $result = $messagePublisher->publish('test-topic', 'Hello Kafka');

        $this->assertTrue($result);

        $connection->disconnect();
    }

    public function testPublishesArrayMessageWithKafka(): void
    {
        $brokers = $_ENV['KAFKA_BROKERS'] ?? 'kafka:9092';
        [$host, $port] = explode(':', $brokers);

        $config = new ConnectionConfig(host: $host, port: (int)$port);
        $connection = new KafkaConnection($config);
        $publisher = new KafkaPublisher($connection);
        $messagePublisher = new MessagePublisher($publisher);

        $result = $messagePublisher->publish('test-topic', [
            'event' => 'order.created',
            'order_id' => 456
        ]);

        $this->assertTrue($result);

        $connection->disconnect();
    }

    public function testPublishesStringMessageWithRabbitMq(): void
    {
        $config = new ConnectionConfig(
            host: $_ENV['RABBITMQ_HOST'] ?? 'rabbitmq',
            port: (int)($_ENV['RABBITMQ_PORT'] ?? 5672),
            username: $_ENV['RABBITMQ_USER'] ?? 'guest',
            password: $_ENV['RABBITMQ_PASSWORD'] ?? 'guest'
        );

        $connection = new RabbitMqConnection($config);
        $publisher = new RabbitMqPublisher($connection);
        $messagePublisher = new MessagePublisher($publisher);

        $result = $messagePublisher->publish('test.routing.key', 'Hello RabbitMQ');

        $this->assertTrue($result);

        $connection->disconnect();
    }

    public function testPublishesArrayMessageWithRabbitMq(): void
    {
        $config = new ConnectionConfig(
            host: $_ENV['RABBITMQ_HOST'] ?? 'rabbitmq',
            port: (int)($_ENV['RABBITMQ_PORT'] ?? 5672),
            username: $_ENV['RABBITMQ_USER'] ?? 'guest',
            password: $_ENV['RABBITMQ_PASSWORD'] ?? 'guest'
        );

        $connection = new RabbitMqConnection($config);
        $publisher = new RabbitMqPublisher($connection);
        $messagePublisher = new MessagePublisher($publisher);

        $result = $messagePublisher->publish('test.routing.key', [
            'notification' => 'Email sent',
            'recipient' => 'user@example.com'
        ]);

        $this->assertTrue($result);

        $connection->disconnect();
    }

    public function testThrowsExceptionForNonSerializableArray(): void
    {
        $config = new ConnectionConfig(
            host: $_ENV['REDIS_HOST'] ?? 'redis',
            port: 6379
        );

        $connection = new RedisConnection($config);
        $publisher = new RedisPublisher($connection);
        $messagePublisher = new MessagePublisher($publisher);

        $resource = fopen('php://memory', 'r');

        $this->expectException(PublishException::class);
        $this->expectExceptionMessage("Cannot serialize resource");

        try {
            $messagePublisher->publish('test.channel', ['file' => $resource]);
        } finally {
            fclose($resource);
            $connection->disconnect();
        }
    }

    public function testThrowsExceptionForClosure(): void
    {
        $config = new ConnectionConfig(
            host: $_ENV['REDIS_HOST'] ?? 'redis',
            port: 6379
        );

        $connection = new RedisConnection($config);
        $publisher = new RedisPublisher($connection);
        $messagePublisher = new MessagePublisher($publisher);

        $this->expectException(PublishException::class);
        $this->expectExceptionMessage("Cannot serialize closure");

        $messagePublisher->publish('test.channel', ['callback' => fn() => 'test']);

        $connection->disconnect();
    }

    public function testPublishesComplexNestedArray(): void
    {
        $config = new ConnectionConfig(
            host: $_ENV['REDIS_HOST'] ?? 'redis',
            port: 6379
        );

        $connection = new RedisConnection($config);
        $publisher = new RedisPublisher($connection);
        $messagePublisher = new MessagePublisher($publisher);

        $complexData = [
            'user' => [
                'id' => 123,
                'name' => 'John Doe',
                'meta' => [
                    'roles' => ['admin', 'user'],
                    'permissions' => [
                        'read' => true,
                        'write' => true,
                        'delete' => false
                    ]
                ]
            ],
            'timestamp' => time(),
            'nullable' => null
        ];

        $result = $messagePublisher->publish('test.channel', $complexData);

        $this->assertTrue($result);

        $connection->disconnect();
    }

    public function testPublishesWithDateTimeObject(): void
    {
        $config = new ConnectionConfig(
            host: $_ENV['REDIS_HOST'] ?? 'redis',
            port: 6379
        );

        $connection = new RedisConnection($config);
        $publisher = new RedisPublisher($connection);
        $messagePublisher = new MessagePublisher($publisher);

        $result = $messagePublisher->publish('test.channel', [
            'event' => 'scheduled',
            'date' => new \DateTime('2024-01-01')
        ]);

        $this->assertTrue($result);

        $connection->disconnect();
    }

    public function testPublishesMultipleMessagesInSequence(): void
    {
        $config = new ConnectionConfig(
            host: $_ENV['REDIS_HOST'] ?? 'redis',
            port: 6379
        );

        $connection = new RedisConnection($config);
        $publisher = new RedisPublisher($connection);
        $messagePublisher = new MessagePublisher($publisher);

        $result1 = $messagePublisher->publish('test.channel', 'message 1');
        $result2 = $messagePublisher->publish('test.channel', ['data' => 'message 2']);
        $result3 = $messagePublisher->publish('test.channel', 'message 3');

        $this->assertTrue($result1);
        $this->assertTrue($result2);
        $this->assertTrue($result3);

        $connection->disconnect();
    }

    public function testSwitchesBetweenPublishers(): void
    {
        // Redis
        $redisConfig = new ConnectionConfig(
            host: $_ENV['REDIS_HOST'] ?? 'redis',
            port: 6379
        );
        $redisConnection = new RedisConnection($redisConfig);
        $redisPublisher = new RedisPublisher($redisConnection);
        $redisMessagePublisher = new MessagePublisher($redisPublisher);

        // Kafka
        $brokers = $_ENV['KAFKA_BROKERS'] ?? 'kafka:9092';
        [$host, $port] = explode(':', $brokers);
        $kafkaConfig = new ConnectionConfig(host: $host, port: (int)$port);
        $kafkaConnection = new KafkaConnection($kafkaConfig);
        $kafkaPublisher = new KafkaPublisher($kafkaConnection);
        $kafkaMessagePublisher = new MessagePublisher($kafkaPublisher);

        // Both should work independently
        $redisResult = $redisMessagePublisher->publish('test.channel', 'Redis message');
        $kafkaResult = $kafkaMessagePublisher->publish('test-topic', 'Kafka message');

        $this->assertTrue($redisResult);
        $this->assertTrue($kafkaResult);

        $redisConnection->disconnect();
        $kafkaConnection->disconnect();
    }

    public function testFluentApiWithRedisFactory(): void
    {
        $redisConn = $this->connectionFactory->redis($_ENV['REDIS_HOST'] ?? 'redis', 6379);
        $publisher = new MessagePublisher($this->factory->redis($redisConn));

        $result = $publisher->publish('test.fluent', 'Hello Fluent API');

        $this->assertTrue($result);
    }

    public function testFluentApiWithKafkaFactory(): void
    {
        $brokers = $_ENV['KAFKA_BROKERS'] ?? 'kafka:9092';
        $kafkaConn = $this->connectionFactory->kafka($brokers);

        $publisher = new MessagePublisher($this->factory->kafka($kafkaConn));

        $result = $publisher->publish('test-fluent-topic', 'Hello Kafka Fluent');

        $this->assertTrue($result);
    }

    public function testFluentApiWithRabbitMqFactory(): void
    {
        $rabbitConn = $this->connectionFactory->rabbitMq(
            $_ENV['RABBITMQ_HOST'] ?? 'rabbitmq',
            (int)($_ENV['RABBITMQ_PORT'] ?? 5672),
            $_ENV['RABBITMQ_USER'] ?? 'guest',
            $_ENV['RABBITMQ_PASSWORD'] ?? 'guest'
        );

        $publisher = new MessagePublisher($this->factory->rabbitMq($rabbitConn));

        $result = $publisher->publish('test.fluent.key', 'Hello RabbitMQ Fluent');

        $this->assertTrue($result);
    }

    public function testFluentApiMultipleLayers(): void
    {
        $brokers = $_ENV['KAFKA_BROKERS'] ?? 'kafka:9092';
        $redisConn = $this->connectionFactory->redis($_ENV['REDIS_HOST'] ?? 'redis', 6379);
        $kafkaConn = $this->connectionFactory->kafka($brokers);

        $publisher = new MessagePublisher(
            $this->factory->redis($redisConn),
            $this->factory->kafka($kafkaConn),
        );

        $result = $publisher->publish('test.multilayer', 'Multi-layer message');

        $this->assertTrue($result);
    }

    public function testThrowsExceptionWhenNoPublishersConfigured(): void
    {
        $publisher = new MessagePublisher();

        $this->expectException(PublishException::class);
        $this->expectExceptionMessage('No publishers configured');

        $publisher->publish('test', 'message');
    }

    public function testPriorityOrdering(): void
    {
        $brokers = $_ENV['KAFKA_BROKERS'] ?? 'kafka:9092';
        $kafkaConn = $this->connectionFactory->kafka($brokers);
        $redisConn = $this->connectionFactory->redis($_ENV['REDIS_HOST'] ?? 'redis', 6379);

        $publisher = new MessagePublisher(
            $this->factory->kafka($kafkaConn),
            $this->factory->redis($redisConn),
        );

        $result = $publisher->publish('test.priority', 'Priority test');

        $this->assertTrue($result);
    }

    public function testPublishWithCustomOptions(): void
    {
        $redisConn = $this->connectionFactory->redis($_ENV['REDIS_HOST'] ?? 'redis', 6379);

        $publisher = new MessagePublisher(
            $this->factory->redis($redisConn, useStreams: true),
        );

        $result = $publisher->publish('test.options', ['custom' => 'data'], ['maxlen' => 1000]);

        $this->assertTrue($result);
    }
}
