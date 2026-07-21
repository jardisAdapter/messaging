<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit\Factory;

use JardisAdapter\Messaging\Connection\DatabaseConnection;
use JardisAdapter\Messaging\Connection\DatabaseConnectionInterface;
use JardisAdapter\Messaging\Connection\ExternalDatabaseConnection;
use JardisAdapter\Messaging\Connection\RedisConnectionInterface;
use JardisAdapter\Messaging\Factory\ConnectionFactory;
use PDO;
use PHPUnit\Framework\TestCase;

class ConnectionFactoryTest extends TestCase
{
    private ConnectionFactory $factory;

    protected function setUp(): void
    {
        $this->factory = new ConnectionFactory();
    }

    public function testRedisReturnsRedisConnectionInterface(): void
    {
        $connection = $this->factory->redis('localhost');

        $this->assertInstanceOf(RedisConnectionInterface::class, $connection);
    }

    public function testDatabaseReturnsDatabaseConnectionInterface(): void
    {
        $connection = $this->factory->database('sqlite::memory:');

        $this->assertInstanceOf(DatabaseConnectionInterface::class, $connection);
        $this->assertInstanceOf(DatabaseConnection::class, $connection);
    }

    public function testFromPdoReturnsDatabaseConnectionInterface(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $connection = $this->factory->fromPdo($pdo);

        $this->assertInstanceOf(DatabaseConnectionInterface::class, $connection);
        $this->assertInstanceOf(ExternalDatabaseConnection::class, $connection);
    }

    public function testFromPdoReturnsConnectedConnection(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $connection = $this->factory->fromPdo($pdo);

        $this->assertTrue($connection->isConnected());
        $this->assertSame($pdo, $connection->getClient());
    }
}
