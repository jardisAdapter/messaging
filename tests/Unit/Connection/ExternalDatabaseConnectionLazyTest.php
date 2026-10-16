<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit\Connection;

use JardisAdapter\Messaging\Connection\ExternalDatabaseConnection;
use JardisAdapter\Messaging\Factory\ConnectionFactory;
use JardisAdapter\Messaging\Publisher\DatabasePublisher;
use JardisSupport\Contract\Messaging\Exception\ConnectionException;
use PDO;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class ExternalDatabaseConnectionLazyTest extends TestCase
{
    private function memoryPdo(): PDO
    {
        $pdo = new PDO('sqlite::memory:');
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        return $pdo;
    }

    public function testReadyPdoFormStillWorks(): void
    {
        $pdo = $this->memoryPdo();
        $connection = new ExternalDatabaseConnection($pdo);

        $this->assertTrue($connection->isConnected());
        $this->assertSame($pdo, $connection->getClient());
    }

    public function testFromDbConnectionDoesNotCallProvider(): void
    {
        $calls = 0;
        $connection = (new ConnectionFactory())->fromDbConnection(function () use (&$calls): PDO {
            $calls++;

            return $this->memoryPdo();
        });

        $this->assertInstanceOf(ExternalDatabaseConnection::class, $connection);
        $this->assertSame(0, $calls);
    }

    public function testIsConnectedConnectAndDisconnectNeverCallProvider(): void
    {
        $calls = 0;
        $connection = new ExternalDatabaseConnection(function () use (&$calls): PDO {
            $calls++;

            return $this->memoryPdo();
        }, manageLifecycle: true);

        $this->assertTrue($connection->isConnected());
        $connection->connect();
        $connection->disconnect();
        $this->assertFalse($connection->isConnected());
        $connection->connect();
        $this->assertTrue($connection->isConnected());

        $this->assertSame(0, $calls);
    }

    public function testFirstGetClientResolvesProviderExactlyOnce(): void
    {
        $calls = 0;
        $pdo = $this->memoryPdo();
        $connection = (new ConnectionFactory())->fromDbConnection(function () use (&$calls, $pdo): PDO {
            $calls++;

            return $pdo;
        });

        $first = $connection->getClient();
        $second = $connection->getClient();
        $connection->isConnected();

        $this->assertSame($pdo, $first);
        $this->assertSame($pdo, $second);
        $this->assertSame(1, $calls);
    }

    public function testProviderExceptionReachesCallerUnchangedAndIsNotMemoized(): void
    {
        $calls = 0;
        $pdo = $this->memoryPdo();
        $failure = new RuntimeException('provider boom');
        $connection = new ExternalDatabaseConnection(function () use (&$calls, $failure, $pdo): PDO {
            $calls++;
            if ($calls === 1) {
                throw $failure;
            }

            return $pdo;
        });

        try {
            $connection->getClient();
            $this->fail('Provider exception was swallowed');
        } catch (RuntimeException $e) {
            $this->assertSame($failure, $e);
        }
        $this->assertSame(1, $calls);

        $this->assertSame($pdo, $connection->getClient());
        $this->assertSame(2, $calls);
        $connection->getClient();
        $this->assertSame(2, $calls);
    }

    public function testDisconnectedManagedConnectionThrowsWithoutCallingProvider(): void
    {
        $calls = 0;
        $connection = new ExternalDatabaseConnection(function () use (&$calls): PDO {
            $calls++;

            return $this->memoryPdo();
        }, manageLifecycle: true);
        $connection->disconnect();

        $this->expectException(ConnectionException::class);
        try {
            $connection->getClient();
        } finally {
            $this->assertSame(0, $calls);
        }
    }

    public function testMemoryPdoFromProviderIsTheCallersDatabase(): void
    {
        $pdo = $this->memoryPdo();
        $pdo->exec(
            'CREATE TABLE domain_events (id INTEGER PRIMARY KEY AUTOINCREMENT, topic TEXT NOT NULL, '
            . 'payload TEXT NOT NULL, created_at TEXT NOT NULL, processed_at TEXT NULL, '
            . 'attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT NULL)'
        );

        $connection = (new ConnectionFactory())->fromDbConnection(static fn (): PDO => $pdo);
        $publisher = new DatabasePublisher($connection);

        $this->assertTrue($publisher->publish('OrderCreated', '{"id":1}'));

        $rows = $pdo->query('SELECT topic, payload FROM domain_events')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame([['topic' => 'OrderCreated', 'payload' => '{"id":1}']], $rows);
        $this->assertSame($pdo, $connection->getClient());
    }
}
