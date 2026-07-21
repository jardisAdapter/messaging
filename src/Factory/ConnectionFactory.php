<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Factory;

use JardisAdapter\Messaging\Config\ConnectionConfig;
use JardisAdapter\Messaging\Connection\DatabaseConnection;
use JardisAdapter\Messaging\Connection\DatabaseConnectionInterface;
use JardisAdapter\Messaging\Connection\ExternalDatabaseConnection;
use JardisAdapter\Messaging\Connection\ExternalKafkaConsumerConnection;
use JardisAdapter\Messaging\Connection\ExternalKafkaProducerConnection;
use JardisAdapter\Messaging\Connection\ExternalRabbitMqConnection;
use JardisAdapter\Messaging\Connection\ExternalRedisConnection;
use JardisAdapter\Messaging\Connection\KafkaConnection;
use JardisAdapter\Messaging\Connection\KafkaConsumerConnection;
use JardisAdapter\Messaging\Connection\KafkaConsumerConnectionInterface;
use JardisAdapter\Messaging\Connection\KafkaProducerConnectionInterface;
use JardisAdapter\Messaging\Connection\RabbitMqConnection;
use JardisAdapter\Messaging\Connection\RabbitMqConnectionInterface;
use JardisAdapter\Messaging\Connection\RedisConnection;
use JardisAdapter\Messaging\Connection\RedisConnectionInterface;
use AMQPConnection;
use PDO;
use RdKafka\KafkaConsumer;
use RdKafka\Producer;
use Redis;

/**
 * Factory for creating connection instances for all supported transports
 */
final class ConnectionFactory
{
    /**
     * @param array<string, mixed> $options
     */
    public function redis(
        string $host,
        int $port = 6379,
        ?string $password = null,
        array $options = []
    ): RedisConnectionInterface {
        return new RedisConnection(
            new ConnectionConfig($host, $port, password: $password, options: $options)
        );
    }

    /**
     * @param array<string, mixed> $options
     */
    public function kafka(
        string $brokers,
        ?string $username = null,
        ?string $password = null,
        array $options = []
    ): KafkaProducerConnectionInterface {
        return new KafkaConnection(
            new ConnectionConfig($brokers, 9092, $username, $password, $options)
        );
    }

    /**
     * @param array<string, mixed> $options
     */
    public function kafkaConsumer(
        string $brokers,
        string $groupId,
        ?string $username = null,
        ?string $password = null,
        array $options = []
    ): KafkaConsumerConnectionInterface {
        return new KafkaConsumerConnection(
            new ConnectionConfig($brokers, 9092, $username, $password, $options),
            $groupId
        );
    }

    /**
     * @param array<string, mixed> $options
     */
    public function rabbitMq(
        string $host,
        int $port = 5672,
        string $username = 'guest',
        string $password = 'guest',
        array $options = []
    ): RabbitMqConnectionInterface {
        $exchangeName = $options['exchange_name'] ?? 'amq.topic';
        $exchangeType = $options['exchange_type'] ?? AMQP_EX_TYPE_TOPIC;
        $exchangeFlags = isset($options['exchange_flags']) ? (int) $options['exchange_flags'] : AMQP_DURABLE;
        unset($options['exchange_name'], $options['exchange_type'], $options['exchange_flags']);

        return new RabbitMqConnection(
            new ConnectionConfig($host, $port, $username, $password, $options),
            $exchangeName,
            $exchangeType,
            $exchangeFlags
        );
    }

    /**
     * @param array<int, mixed> $options
     */
    public function database(
        string $dsn,
        ?string $username = null,
        ?string $password = null,
        array $options = []
    ): DatabaseConnectionInterface {
        return new DatabaseConnection($dsn, $username, $password, $options);
    }

    public function fromRedis(
        Redis $redis,
        bool $manageLifecycle = false
    ): RedisConnectionInterface {
        return new ExternalRedisConnection($redis, $manageLifecycle);
    }

    public function fromPdo(
        PDO $pdo,
        bool $manageLifecycle = false
    ): DatabaseConnectionInterface {
        return new ExternalDatabaseConnection($pdo, $manageLifecycle);
    }

    public function fromAmqp(
        AMQPConnection $connection,
        string $exchangeName = 'amq.direct',
        string $exchangeType = AMQP_EX_TYPE_DIRECT,
        bool $manageLifecycle = false,
        int $exchangeFlags = AMQP_DURABLE
    ): RabbitMqConnectionInterface {
        return new ExternalRabbitMqConnection(
            $connection,
            $exchangeName,
            $exchangeType,
            $manageLifecycle,
            $exchangeFlags
        );
    }

    public function fromKafkaProducer(
        Producer $producer,
        bool $flushOnDisconnect = false
    ): KafkaProducerConnectionInterface {
        return new ExternalKafkaProducerConnection($producer, $flushOnDisconnect);
    }

    public function fromKafkaConsumer(
        KafkaConsumer $consumer
    ): KafkaConsumerConnectionInterface {
        return new ExternalKafkaConsumerConnection($consumer);
    }
}
