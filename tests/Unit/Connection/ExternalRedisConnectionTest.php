<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit\Connection;

use JardisAdapter\Messaging\Connection\ExternalRedisConnection;
use JardisSupport\Contract\Messaging\Exception\ConnectionException;
use PHPUnit\Framework\TestCase;
use Redis;

class ExternalRedisConnectionTest extends TestCase
{
    private Redis $redis;

    protected function setUp(): void
    {
        $this->redis = $this->createMock(Redis::class);
    }

    public function testConstructorAcceptsExternalRedis(): void
    {
        $connection = new ExternalRedisConnection($this->redis);

        $this->assertInstanceOf(ExternalRedisConnection::class, $connection);
    }

    public function testIsConnectedReturnsTrueWhenRedisIsAlive(): void
    {
        $this->redis->method('ping')->willReturn('+PONG');

        $connection = new ExternalRedisConnection($this->redis);

        $this->assertTrue($connection->isConnected());
    }

    public function testIsConnectedReturnsFalseWhenRedisPingFails(): void
    {
        $this->redis->method('ping')->willReturn(false);

        $connection = new ExternalRedisConnection($this->redis);

        $this->assertFalse($connection->isConnected());
    }

    public function testIsConnectedReturnsFalseWhenRedisThrowsException(): void
    {
        $this->redis->method('ping')->willThrowException(new \RedisException('Connection lost'));

        $connection = new ExternalRedisConnection($this->redis);

        $this->assertFalse($connection->isConnected());
    }

    public function testGetClientReturnsExternalRedis(): void
    {
        $this->redis->method('ping')->willReturn('+PONG');

        $connection = new ExternalRedisConnection($this->redis);

        $this->assertSame($this->redis, $connection->getClient());
    }

    public function testConnectIsNoOp(): void
    {
        $this->redis->method('ping')->willReturn('+PONG');

        $connection = new ExternalRedisConnection($this->redis);

        // Should not throw exception
        $connection->connect();

        $this->assertTrue($connection->isConnected());
    }

    public function testDisconnectIsNoOpWithoutLifecycleManagement(): void
    {
        $this->redis->expects($this->never())->method('close');
        $this->redis->method('ping')->willReturn('+PONG');

        $connection = new ExternalRedisConnection($this->redis, manageLifecycle: false);
        $connection->disconnect();

        $this->assertTrue($connection->isConnected());
    }

    public function testDisconnectClosesWhenLifecycleManaged(): void
    {
        $this->redis->expects($this->once())->method('close');

        $connection = new ExternalRedisConnection($this->redis, manageLifecycle: true);
        $connection->disconnect();

        $this->assertFalse($connection->isConnected());
    }

    public function testDisconnectIgnoresRedisExceptionWhenClosing(): void
    {
        $this->redis->method('close')->willThrowException(new \RedisException('Already closed'));

        $connection = new ExternalRedisConnection($this->redis, manageLifecycle: true);

        // Should not throw exception
        $connection->disconnect();

        $this->assertFalse($connection->isConnected());
    }

    public function testGetClientThrowsExceptionWhenDisconnected(): void
    {
        $this->redis->method('ping')->willReturn('+PONG');

        $connection = new ExternalRedisConnection($this->redis, manageLifecycle: true);
        $connection->disconnect();

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('External Redis connection is not available');

        $connection->getClient();
    }

    public function testGetClientThrowsExceptionWhenRedisNotAlive(): void
    {
        $this->redis->method('ping')->willReturn(false);

        $connection = new ExternalRedisConnection($this->redis);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('External Redis connection is not available');

        $connection->getClient();
    }

    public function testConnectAfterDisconnectWithLifecycleManagement(): void
    {
        $this->redis->method('ping')->willReturn('+PONG');

        $connection = new ExternalRedisConnection($this->redis, manageLifecycle: true);
        $connection->disconnect();

        $this->assertFalse($connection->isConnected());

        $connection->connect();

        $this->assertTrue($connection->isConnected());
    }
}
