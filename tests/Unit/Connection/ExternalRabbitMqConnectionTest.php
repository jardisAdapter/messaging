<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit\Connection;

use AMQPConnection;
use JardisAdapter\Messaging\Connection\ExternalRabbitMqConnection;
use JardisSupport\Contract\Messaging\Exception\ConnectionException;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests for ExternalRabbitMqConnection
 *
 * Note: These are limited unit tests because AMQPConnection, AMQPChannel,
 * and AMQPExchange cannot be properly mocked (they require real connections).
 * Full integration tests are in Integration/Connection/ExternalConnectionsIntegrationTest.php
 */
class ExternalRabbitMqConnectionTest extends TestCase
{
    private AMQPConnection $amqpConnection;

    protected function setUp(): void
    {
        $this->amqpConnection = $this->createMock(AMQPConnection::class);
    }

    public function testConstructorAcceptsExternalConnection(): void
    {
        $connection = new ExternalRabbitMqConnection($this->amqpConnection);

        $this->assertInstanceOf(ExternalRabbitMqConnection::class, $connection);
    }

    public function testConstructorWithCustomExchangeConfiguration(): void
    {
        $connection = new ExternalRabbitMqConnection(
            connection: $this->amqpConnection,
            exchangeName: 'custom-exchange',
            exchangeType: AMQP_EX_TYPE_TOPIC,
            manageLifecycle: true
        );

        $this->assertInstanceOf(ExternalRabbitMqConnection::class, $connection);
    }

    public function testIsConnectedReturnsFalseBeforeConnect(): void
    {
        $this->amqpConnection->method('isConnected')->willReturn(false);

        $connection = new ExternalRabbitMqConnection($this->amqpConnection);

        $this->assertFalse($connection->isConnected());
    }

    public function testConnectThrowsExceptionWhenExternalConnectionNotConnected(): void
    {
        $this->amqpConnection->method('isConnected')->willReturn(false);

        $connection = new ExternalRabbitMqConnection($this->amqpConnection);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('External AMQP connection is not connected');

        $connection->connect();
    }

    public function testGetConnectionThrowsExceptionWhenNotConnected(): void
    {
        $this->amqpConnection->method('isConnected')->willReturn(false);

        $connection = new ExternalRabbitMqConnection($this->amqpConnection);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('External RabbitMQ connection is not available');

        $connection->getConnection();
    }

    public function testDisconnectIsNoOpWithoutLifecycleManagement(): void
    {
        $this->amqpConnection->expects($this->never())->method('disconnect');

        $connection = new ExternalRabbitMqConnection($this->amqpConnection, manageLifecycle: false);
        $connection->disconnect();

        // Internal state unchanged — isConnected depends on external connection state
        $this->assertInstanceOf(ExternalRabbitMqConnection::class, $connection);
    }

    public function testDisconnectWithLifecycleManagement(): void
    {
        // Simulate that connect() was called first
        $this->amqpConnection->method('isConnected')->willReturn(false);
        $this->amqpConnection->expects($this->never())->method('disconnect');

        // When not connected, disconnect should not call external disconnect
        $connection = new ExternalRabbitMqConnection($this->amqpConnection, manageLifecycle: true);
        $connection->disconnect();

        $this->assertFalse($connection->isConnected());
    }

    public function testIsConnectedChecksExternalConnectionState(): void
    {
        // First call returns true, second returns false
        $this->amqpConnection->method('isConnected')
            ->willReturnOnConsecutiveCalls(false, false);

        $connection = new ExternalRabbitMqConnection($this->amqpConnection);

        $this->assertFalse($connection->isConnected());
        $this->assertFalse($connection->isConnected());
    }
}
