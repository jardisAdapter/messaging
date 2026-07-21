<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Integration\Connection;

use JardisAdapter\Messaging\Config\ConnectionConfig;
use JardisAdapter\Messaging\Connection\RedisConnection;
use JardisSupport\Contract\Messaging\Exception\ConnectionException;
use PHPUnit\Framework\TestCase;

class RedisConnectionTest extends TestCase
{
    private ConnectionConfig $config;

    protected function setUp(): void
    {
        $this->config = new ConnectionConfig(
            host: $_ENV['REDIS_HOST'] ?? 'redis',
            port: 6379 // Internal Docker port
        );
    }

    public function testConnectsSuccessfully(): void
    {
        $connection = new RedisConnection($this->config);
        $connection->connect();

        $this->assertTrue($connection->isConnected());

        $connection->disconnect();
    }

    public function testIsNotConnectedInitially(): void
    {
        $connection = new RedisConnection($this->config);

        $this->assertFalse($connection->isConnected());
    }

    public function testDisconnectsSuccessfully(): void
    {
        $connection = new RedisConnection($this->config);
        $connection->connect();

        $this->assertTrue($connection->isConnected());

        $connection->disconnect();

        $this->assertFalse($connection->isConnected());
    }

    public function testReconnectsAfterDisconnect(): void
    {
        $connection = new RedisConnection($this->config);

        $connection->connect();
        $connection->disconnect();
        $connection->connect();

        $this->assertTrue($connection->isConnected());

        $connection->disconnect();
    }

    public function testConnectIsIdempotent(): void
    {
        $connection = new RedisConnection($this->config);

        $connection->connect();
        $connection->connect(); // Should not throw
        $connection->connect(); // Should not throw

        $this->assertTrue($connection->isConnected());

        $connection->disconnect();
    }

    public function testDisconnectIsIdempotent(): void
    {
        $connection = new RedisConnection($this->config);
        $connection->connect();

        $connection->disconnect();
        $connection->disconnect(); // Should not throw
        $connection->disconnect(); // Should not throw

        $this->assertFalse($connection->isConnected());
    }

    public function testGetClientReturnsRedisInstance(): void
    {
        $connection = new RedisConnection($this->config);
        $connection->connect();

        $client = $connection->getClient();

        $this->assertInstanceOf(\Redis::class, $client);

        $connection->disconnect();
    }

    public function testGetClientThrowsWhenNotConnected(): void
    {
        $connection = new RedisConnection($this->config);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Not connected to Redis');

        $connection->getClient();
    }

    public function testClientCanPerformOperations(): void
    {
        $connection = new RedisConnection($this->config);
        $connection->connect();

        $client = $connection->getClient();
        $client->set('test_key', 'test_value');
        $value = $client->get('test_key');

        $this->assertSame('test_value', $value);

        $client->del('test_key');
        $connection->disconnect();
    }

    public function testThrowsExceptionForInvalidHost(): void
    {
        $config = new ConnectionConfig('invalid.host.that.does.not.exist', 6380);
        $connection = new RedisConnection($config);

        $this->expectException(ConnectionException::class);

        $connection->connect();
    }

    public function testThrowsExceptionForInvalidPort(): void
    {
        $config = new ConnectionConfig($_ENV['REDIS_HOST'] ?? 'redis', 9999);
        $connection = new RedisConnection($config);

        $this->expectException(ConnectionException::class);

        $connection->connect();
    }

    public function testDisconnectsOnDestruct(): void
    {
        $connection = new RedisConnection($this->config);
        $connection->connect();

        $this->assertTrue($connection->isConnected());

        unset($connection); // Trigger __destruct

        // Create new connection to verify previous was cleaned up
        $newConnection = new RedisConnection($this->config);
        $newConnection->connect();

        $this->assertTrue($newConnection->isConnected());

        $newConnection->disconnect();
    }

    public function testSelectsDatabase(): void
    {
        $config = new ConnectionConfig(
            host: $_ENV['REDIS_HOST'] ?? 'redis',
            port: 6379, // Internal Docker port
            options: ['database' => 1]
        );

        $connection = new RedisConnection($config);
        $connection->connect();

        $client = $connection->getClient();
        $client->set('db_test', 'value');

        $this->assertSame('value', $client->get('db_test'));

        $client->del('db_test');
        $connection->disconnect();
    }
}
