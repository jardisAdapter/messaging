<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit;

use JardisAdapter\Messaging\Config\DatabaseTransportOptions;
use JardisAdapter\Messaging\Connection\DatabaseConnection;
use JardisAdapter\Messaging\Connection\ExternalDatabaseConnection;
use JardisAdapter\Messaging\Publisher\DatabasePublisher;
use JardisSupport\Contract\Messaging\Exception\PublishException;
use PDO;
use PHPUnit\Framework\TestCase;

class DatabasePublisherTest extends TestCase
{
    private PDO $pdo;
    private ExternalDatabaseConnection $connection;

    protected function setUp(): void
    {
        $this->pdo = new PDO('sqlite::memory:');
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->createTable($this->pdo);

        $this->connection = new ExternalDatabaseConnection($this->pdo);
    }

    public function testPublishInsertsMessageIntoTable(): void
    {
        $publisher = new DatabasePublisher($this->connection);

        $result = $publisher->publish('OrderCreated', '{"id": 1}');

        $this->assertTrue($result);

        $stmt = $this->pdo->query('SELECT * FROM domain_events');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('OrderCreated', $rows[0]['topic']);
        $this->assertSame('{"id": 1}', $rows[0]['payload']);
        $this->assertNull($rows[0]['processed_at']);
        $this->assertSame('0', (string) $rows[0]['attempts']);
    }

    public function testPublishMultipleMessages(): void
    {
        $publisher = new DatabasePublisher($this->connection);

        $publisher->publish('OrderCreated', '{"id": 1}');
        $publisher->publish('OrderCreated', '{"id": 2}');
        $publisher->publish('ArticleChanged', '{"id": 3}');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM domain_events');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->prepare('SELECT COUNT(*) FROM domain_events WHERE topic = ?');
        $stmt->execute(['OrderCreated']);
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    public function testPublishSetsCreatedAtTimestamp(): void
    {
        $publisher = new DatabasePublisher($this->connection);

        $publisher->publish('TestEvent', 'data');

        $stmt = $this->pdo->query('SELECT created_at FROM domain_events');
        $createdAt = $stmt->fetchColumn();

        $this->assertNotNull($createdAt);
        $this->assertNotEmpty($createdAt);
    }

    public function testPublishWithCustomTable(): void
    {
        $this->createTable($this->pdo, 'custom_events');

        $options = new DatabaseTransportOptions(table: 'custom_events');
        $publisher = new DatabasePublisher($this->connection, $options);

        $publisher->publish('TestEvent', 'data');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM custom_events');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    public function testPublishThrowsPublishExceptionOnDatabaseError(): void
    {
        $this->expectException(PublishException::class);
        $this->expectExceptionMessage('Failed to publish message to database');

        $pdo = new PDO('sqlite::memory:');
        $connection = new ExternalDatabaseConnection($pdo);
        $publisher = new DatabasePublisher($connection);

        $publisher->publish('TestEvent', 'data');
    }

    public function testGetConnectionReturnsConnection(): void
    {
        $publisher = new DatabasePublisher($this->connection);

        $this->assertSame($this->connection, $publisher->getConnection());
    }

    public function testPublishWithDatabaseConnection(): void
    {
        $connection = new DatabaseConnection('sqlite::memory:');
        $connection->connect();
        $publisher = new DatabasePublisher($connection);

        $pdo = $connection->getClient();
        $this->createTable($pdo);

        $result = $publisher->publish('TestEvent', '{"test": true}');
        $this->assertTrue($result);

        $stmt = $pdo->query('SELECT COUNT(*) FROM domain_events');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    private function createTable(PDO $pdo, string $table = 'domain_events'): void
    {
        $pdo->exec(
            "CREATE TABLE {$table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic VARCHAR(255) NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                processed_at TEXT NULL DEFAULT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NULL DEFAULT NULL
            )"
        );
    }
}
