<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Integration\Connection;

use AMQPChannel;
use AMQPConnection;
use AMQPExchange;
use JardisAdapter\Messaging\Config\ConnectionConfig;
use JardisAdapter\Messaging\Connection\RabbitMqConnection;
use JardisSupport\Contract\Messaging\Exception\ConnectionException;
use PHPUnit\Framework\TestCase;

class RabbitMqConnectionTest extends TestCase
{
    private ConnectionConfig $config;

    protected function setUp(): void
    {
        $this->config = new ConnectionConfig(
            host: $_ENV['RABBITMQ_HOST'] ?? 'rabbitmq',
            port: (int)($_ENV['RABBITMQ_PORT'] ?? 5672),
            username: $_ENV['RABBITMQ_USER'] ?? 'guest',
            password: $_ENV['RABBITMQ_PASSWORD'] ?? 'guest'
        );
    }

    public function testConnectsSuccessfully(): void
    {
        $connection = new RabbitMqConnection($this->config);
        $connection->connect();

        $this->assertTrue($connection->isConnected());

        $connection->disconnect();
    }

    public function testIsNotConnectedInitially(): void
    {
        $connection = new RabbitMqConnection($this->config);

        $this->assertFalse($connection->isConnected());
    }

    public function testDisconnectsSuccessfully(): void
    {
        $connection = new RabbitMqConnection($this->config);
        $connection->connect();

        $this->assertTrue($connection->isConnected());

        $connection->disconnect();

        $this->assertFalse($connection->isConnected());
    }

    public function testReconnectsAfterDisconnect(): void
    {
        $connection = new RabbitMqConnection($this->config);

        $connection->connect();
        $connection->disconnect();
        $connection->connect();

        $this->assertTrue($connection->isConnected());

        $connection->disconnect();
    }

    public function testConnectIsIdempotent(): void
    {
        $connection = new RabbitMqConnection($this->config);

        $connection->connect();
        $connection->connect(); // Should not throw
        $connection->connect(); // Should not throw

        $this->assertTrue($connection->isConnected());

        $connection->disconnect();
    }

    public function testDisconnectIsIdempotent(): void
    {
        $connection = new RabbitMqConnection($this->config);
        $connection->connect();

        $connection->disconnect();
        $connection->disconnect(); // Should not throw
        $connection->disconnect(); // Should not throw

        $this->assertFalse($connection->isConnected());
    }

    public function testGetExchangeReturnsAMQPExchange(): void
    {
        $connection = new RabbitMqConnection($this->config);
        $connection->connect();

        $exchange = $connection->getExchange();

        $this->assertInstanceOf(AMQPExchange::class, $exchange);

        $connection->disconnect();
    }

    public function testGetChannelReturnsAMQPChannel(): void
    {
        $connection = new RabbitMqConnection($this->config);
        $connection->connect();

        $channel = $connection->getChannel();

        $this->assertInstanceOf(AMQPChannel::class, $channel);

        $connection->disconnect();
    }

    public function testGetConnectionReturnsAMQPConnection(): void
    {
        $connection = new RabbitMqConnection($this->config);
        $connection->connect();

        $amqpConnection = $connection->getConnection();

        $this->assertInstanceOf(AMQPConnection::class, $amqpConnection);

        $connection->disconnect();
    }

    public function testGetExchangeThrowsWhenNotConnected(): void
    {
        $connection = new RabbitMqConnection($this->config);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Not connected to RabbitMQ');

        $connection->getExchange();
    }

    public function testGetChannelThrowsWhenNotConnected(): void
    {
        $connection = new RabbitMqConnection($this->config);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Not connected to RabbitMQ');

        $connection->getChannel();
    }

    public function testGetConnectionThrowsWhenNotConnected(): void
    {
        $connection = new RabbitMqConnection($this->config);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Not connected to RabbitMQ');

        $connection->getConnection();
    }

    public function testThrowsExceptionForInvalidHost(): void
    {
        $config = new ConnectionConfig(
            'invalid.host.that.does.not.exist',
            5672,
            'guest',
            'guest'
        );
        $connection = new RabbitMqConnection($config);

        $this->expectException(ConnectionException::class);

        $connection->connect();
    }

    public function testThrowsExceptionForInvalidCredentials(): void
    {
        $config = new ConnectionConfig(
            $_ENV['RABBITMQ_HOST'] ?? 'rabbitmq',
            (int)($_ENV['RABBITMQ_PORT'] ?? 5672),
            'invalid_user',
            'invalid_pass'
        );
        $connection = new RabbitMqConnection($config);

        $this->expectException(ConnectionException::class);

        $connection->connect();
    }

    public function testDisconnectsOnDestruct(): void
    {
        $connection = new RabbitMqConnection($this->config);
        $connection->connect();

        $this->assertTrue($connection->isConnected());

        unset($connection); // Trigger __destruct

        // Create new connection to verify previous was cleaned up
        $newConnection = new RabbitMqConnection($this->config);
        $newConnection->connect();

        $this->assertTrue($newConnection->isConnected());

        $newConnection->disconnect();
    }

    public function testCreatesCustomExchange(): void
    {
        $connection = new RabbitMqConnection(
            config: $this->config,
            exchangeName: 'test.exchange',
            exchangeType: AMQP_EX_TYPE_DIRECT,
            exchangeFlags: AMQP_DURABLE
        );

        $connection->connect();

        $exchange = $connection->getExchange();
        $this->assertSame('test.exchange', $exchange->getName());
        $this->assertSame(AMQP_EX_TYPE_DIRECT, $exchange->getType());

        $connection->disconnect();
    }

    public function testConnectsWithVhost(): void
    {
        $config = new ConnectionConfig(
            host: $_ENV['RABBITMQ_HOST'] ?? 'rabbitmq',
            port: (int)($_ENV['RABBITMQ_PORT'] ?? 5672),
            username: $_ENV['RABBITMQ_USER'] ?? 'guest',
            password: $_ENV['RABBITMQ_PASSWORD'] ?? 'guest',
            options: ['vhost' => '/']
        );

        $connection = new RabbitMqConnection($config);
        $connection->connect();

        $this->assertTrue($connection->isConnected());

        $connection->disconnect();
    }
}
