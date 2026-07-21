<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit;

use JardisAdapter\Messaging\Connection\DatabaseConnection;
use JardisAdapter\Messaging\Connection\ExternalDatabaseConnection;
use JardisSupport\Contract\Messaging\Exception\ConnectionException;
use PDO;
use PHPUnit\Framework\TestCase;

class DatabaseConnectionTest extends TestCase
{
    public function testConnectWithSqliteInMemory(): void
    {
        $connection = new DatabaseConnection('sqlite::memory:');
        $connection->connect();

        $this->assertTrue($connection->isConnected());
    }

    public function testDisconnect(): void
    {
        $connection = new DatabaseConnection('sqlite::memory:');
        $connection->connect();
        $connection->disconnect();

        $this->assertFalse($connection->isConnected());
    }

    public function testGetClientThrowsWhenNotConnected(): void
    {
        $connection = new DatabaseConnection('sqlite::memory:');

        $this->assertFalse($connection->isConnected());

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Not connected to database. Call connect() first.');

        $connection->getClient();
    }

    public function testGetClientReturnsPdoAfterConnect(): void
    {
        $connection = new DatabaseConnection('sqlite::memory:');
        $connection->connect();

        $pdo = $connection->getClient();

        $this->assertInstanceOf(PDO::class, $pdo);
    }

    public function testGetClientReturnsSameInstance(): void
    {
        $connection = new DatabaseConnection('sqlite::memory:');
        $connection->connect();

        $pdo1 = $connection->getClient();
        $pdo2 = $connection->getClient();

        $this->assertSame($pdo1, $pdo2);
    }

    public function testConnectDoesNotReconnectIfAlreadyConnected(): void
    {
        $connection = new DatabaseConnection('sqlite::memory:');
        $connection->connect();
        $pdo1 = $connection->getClient();

        $connection->connect();
        $pdo2 = $connection->getClient();

        $this->assertSame($pdo1, $pdo2);
    }

    public function testThrowsConnectionExceptionForInvalidDsn(): void
    {
        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Database connection error');

        $connection = new DatabaseConnection('invalid:dsn');
        $connection->connect();
    }

    public function testIsConnectedReturnsFalseBeforeConnect(): void
    {
        $connection = new DatabaseConnection('sqlite::memory:');

        $this->assertFalse($connection->isConnected());
    }

    public function testExternalConnectionIsConnectedByDefault(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $external = new ExternalDatabaseConnection($pdo);

        $this->assertTrue($external->isConnected());
    }

    public function testExternalConnectionGetClientReturnsPdo(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $external = new ExternalDatabaseConnection($pdo);

        $this->assertSame($pdo, $external->getClient());
    }

    public function testExternalConnectionDisconnectWithoutLifecycleManagement(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $external = new ExternalDatabaseConnection($pdo, manageLifecycle: false);

        $external->disconnect();

        $this->assertTrue($external->isConnected());
    }

    public function testExternalConnectionDisconnectWithLifecycleManagement(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $external = new ExternalDatabaseConnection($pdo, manageLifecycle: true);

        $external->disconnect();

        $this->assertFalse($external->isConnected());
    }

    public function testExternalConnectionThrowsWhenDisconnected(): void
    {
        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('External database connection is not available');

        $pdo = new PDO('sqlite::memory:');
        $external = new ExternalDatabaseConnection($pdo, manageLifecycle: true);
        $external->disconnect();

        $external->getClient();
    }

    public function testExternalConnectionReconnect(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $external = new ExternalDatabaseConnection($pdo, manageLifecycle: true);

        $external->disconnect();
        $this->assertFalse($external->isConnected());

        $external->connect();
        $this->assertTrue($external->isConnected());
    }
}
