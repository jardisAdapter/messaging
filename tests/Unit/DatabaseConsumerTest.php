<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit;

use JardisAdapter\Messaging\Config\DatabaseTransportOptions;
use JardisAdapter\Messaging\Connection\ExternalDatabaseConnection;
use JardisAdapter\Messaging\Consumer\DatabaseConsumer;
use PDO;
use PHPUnit\Framework\TestCase;

class DatabaseConsumerTest extends TestCase
{
    private PDO $pdo;
    private ExternalDatabaseConnection $connection;

    protected function setUp(): void
    {
        $this->pdo = new PDO('sqlite::memory:');
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->createTable($this->pdo);
        $this->createSubscriptionTable($this->pdo);

        $this->connection = new ExternalDatabaseConnection($this->pdo);
    }

    public function testConsumeProcessesUnprocessedMessages(): void
    {
        $this->insertEvent('OrderCreated', '{"id": 1}');
        $this->insertEvent('OrderCreated', '{"id": 2}');

        $options = new DatabaseTransportOptions(pollingIntervalMs: 0);
        $consumer = new DatabaseConsumer($this->connection, $options);

        $messages = [];
        $consumer->consume('OrderCreated', function (string $message, array $metadata) use (&$messages, $consumer): bool {
            $messages[] = $message;
            if (count($messages) >= 2) {
                $consumer->stop();
            }
            return true;
        });

        $this->assertCount(2, $messages);
        $this->assertSame('{"id": 1}', $messages[0]);
        $this->assertSame('{"id": 2}', $messages[1]);
    }

    public function testConsumeSoftDeleteSetsProcessedAt(): void
    {
        $this->insertEvent('OrderCreated', '{"id": 1}');

        $options = new DatabaseTransportOptions(
            deleteAfterProcessing: false,
            pollingIntervalMs: 0,
        );
        $consumer = new DatabaseConsumer($this->connection, $options);

        $consumer->consume('OrderCreated', function () use ($consumer): bool {
            $consumer->stop();
            return true;
        });

        $stmt = $this->pdo->query('SELECT processed_at FROM domain_events WHERE id = 1');
        $processedAt = $stmt->fetchColumn();

        $this->assertNotNull($processedAt);
        $this->assertNotEmpty($processedAt);
    }

    public function testConsumeHardDeleteRemovesRow(): void
    {
        $this->insertEvent('OrderCreated', '{"id": 1}');

        $options = new DatabaseTransportOptions(
            deleteAfterProcessing: true,
            pollingIntervalMs: 0,
        );
        $consumer = new DatabaseConsumer($this->connection, $options);

        $consumer->consume('OrderCreated', function () use ($consumer): bool {
            $consumer->stop();
            return true;
        });

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM domain_events');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public function testConsumeIncrementsAttemptsOnFailure(): void
    {
        $this->insertEvent('OrderCreated', '{"id": 1}');

        $options = new DatabaseTransportOptions(pollingIntervalMs: 0, maxAttempts: 3);
        $consumer = new DatabaseConsumer($this->connection, $options);

        $consumer->consume('OrderCreated', function (): bool {
            return false;
        });

        $stmt = $this->pdo->query('SELECT attempts, last_error FROM domain_events WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertSame(1, (int) $row['attempts']);
        $this->assertSame('Handler returned false', $row['last_error']);
    }

    public function testConsumeIncrementsAttemptsOnException(): void
    {
        $this->insertEvent('OrderCreated', '{"id": 1}');

        $options = new DatabaseTransportOptions(pollingIntervalMs: 0, maxAttempts: 2);
        $consumer = new DatabaseConsumer($this->connection, $options);

        try {
            $consumer->consume('OrderCreated', function (): bool {
                throw new \RuntimeException('Processing failed');
            });
            $this->fail('Expected RuntimeException to be thrown');
        } catch (\RuntimeException $e) {
            $this->assertSame('Processing failed', $e->getMessage());
        }

        $stmt = $this->pdo->query('SELECT attempts, last_error FROM domain_events WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertSame(1, (int) $row['attempts']);
        $this->assertSame('Processing failed', $row['last_error']);
    }

    public function testConsumeSkipsMessagesExceedingMaxAttempts(): void
    {
        // Insert one processable and one exhausted event
        $this->insertEvent('OrderCreated', '{"id": 1}', attempts: 3);
        $this->insertEvent('OrderCreated', '{"id": 2}', attempts: 0);

        $options = new DatabaseTransportOptions(pollingIntervalMs: 0, maxAttempts: 3);
        $consumer = new DatabaseConsumer($this->connection, $options);

        $messages = [];
        $consumer->consume('OrderCreated', function (string $message) use (&$messages, $consumer): bool {
            $messages[] = $message;
            $consumer->stop();
            return true;
        });

        // Only the second event (attempts=0) should be processed
        $this->assertCount(1, $messages);
        $this->assertSame('{"id": 2}', $messages[0]);
    }

    public function testConsumeOnlyProcessesMatchingTopic(): void
    {
        $this->insertEvent('OrderCreated', '{"id": 1}');
        $this->insertEvent('ArticleChanged', '{"id": 2}');

        $options = new DatabaseTransportOptions(pollingIntervalMs: 0);
        $consumer = new DatabaseConsumer($this->connection, $options);

        $messages = [];
        $consumer->consume('OrderCreated', function (string $message) use (&$messages, $consumer): bool {
            $messages[] = $message;
            $consumer->stop();
            return true;
        });

        $this->assertCount(1, $messages);
        $this->assertSame('{"id": 1}', $messages[0]);
    }

    public function testConsumePassesCorrectMetadata(): void
    {
        $this->insertEvent('OrderCreated', '{"id": 1}');

        $options = new DatabaseTransportOptions(pollingIntervalMs: 0);
        $consumer = new DatabaseConsumer($this->connection, $options);

        $receivedMetadata = [];
        $consumer->consume('OrderCreated', function (string $message, array $metadata) use (&$receivedMetadata, $consumer): bool {
            $receivedMetadata = $metadata;
            $consumer->stop();
            return true;
        });

        $this->assertSame(1, $receivedMetadata['id']);
        $this->assertSame('OrderCreated', $receivedMetadata['topic']);
        $this->assertSame(0, $receivedMetadata['attempts']);
        $this->assertSame('database', $receivedMetadata['transport']);
        $this->assertArrayHasKey('created_at', $receivedMetadata);
    }

    public function testConsumeBatchSize(): void
    {
        for ($i = 1; $i <= 5; $i++) {
            $this->insertEvent('OrderCreated', "{\"id\": {$i}}");
        }

        $options = new DatabaseTransportOptions(pollingIntervalMs: 0, batchSize: 2);
        $consumer = new DatabaseConsumer($this->connection, $options);

        $messages = [];
        $consumer->consume('OrderCreated', function (string $message) use (&$messages, $consumer): bool {
            $messages[] = $message;
            if (count($messages) >= 5) {
                $consumer->stop();
            }
            return true;
        });

        $this->assertCount(5, $messages);
    }

    public function testConsumeWithCustomTable(): void
    {
        $this->createTable($this->pdo, 'custom_events');

        $stmt = $this->pdo->prepare(
            "INSERT INTO custom_events (topic, payload, created_at) VALUES (?, ?, datetime('now'))"
        );
        $stmt->execute(['TestEvent', '{"custom": true}']);

        $options = new DatabaseTransportOptions(table: 'custom_events', pollingIntervalMs: 0);
        $consumer = new DatabaseConsumer($this->connection, $options);

        $received = null;
        $consumer->consume('TestEvent', function (string $message) use (&$received, $consumer): bool {
            $received = $message;
            $consumer->stop();
            return true;
        });

        $this->assertSame('{"custom": true}', $received);
    }

    public function testStopPreventsProcessing(): void
    {
        $this->insertEvent('OrderCreated', '{"id": 1}');
        $this->insertEvent('OrderCreated', '{"id": 2}');
        $this->insertEvent('OrderCreated', '{"id": 3}');

        $options = new DatabaseTransportOptions(pollingIntervalMs: 0);
        $consumer = new DatabaseConsumer($this->connection, $options);

        $messages = [];
        $consumer->consume('OrderCreated', function (string $message) use (&$messages, $consumer): bool {
            $messages[] = $message;
            $consumer->stop();
            return true;
        });

        $this->assertCount(1, $messages);
    }

    public function testGetConnectionReturnsConnection(): void
    {
        $consumer = new DatabaseConsumer($this->connection);

        $this->assertSame($this->connection, $consumer->getConnection());
    }

    // -------------------------------------------------------------------------
    // Fan-Out Tests (Consumer Groups)
    // -------------------------------------------------------------------------

    public function testConsumeWithGroupProcessesEvent(): void
    {
        $this->insertEvent('InvoiceCreated', '{"id": 1}');

        $options = new DatabaseTransportOptions(pollingIntervalMs: 0);
        $consumer = new DatabaseConsumer($this->connection, $options);

        $received = null;
        $consumer->consume('InvoiceCreated', function (string $message) use (&$received, $consumer): bool {
            $received = $message;
            $consumer->stop();
            return true;
        }, ['group' => 'email-service']);

        $this->assertSame('{"id": 1}', $received);

        // Verify subscription row was created
        $stmt = $this->pdo->query('SELECT * FROM domain_event_subscriptions');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('1', (string) $rows[0]['event_id']);
        $this->assertSame('email-service', $rows[0]['consumer_group']);
        $this->assertNotNull($rows[0]['processed_at']);
    }

    public function testConsumeWithMultipleGroupsProcessesSameEvent(): void
    {
        $this->insertEvent('InvoiceCreated', '{"id": 1}');

        $options = new DatabaseTransportOptions(pollingIntervalMs: 0);

        // Group A processes the event
        $consumerA = new DatabaseConsumer($this->connection, $options);
        $consumerA->consume('InvoiceCreated', function () use ($consumerA): bool {
            $consumerA->stop();
            return true;
        }, ['group' => 'email-service']);

        // Group B should also see and process the same event
        $consumerB = new DatabaseConsumer($this->connection, $options);
        $receivedByB = null;
        $consumerB->consume('InvoiceCreated', function (string $message) use (&$receivedByB, $consumerB): bool {
            $receivedByB = $message;
            $consumerB->stop();
            return true;
        }, ['group' => 'pdf-service']);

        $this->assertSame('{"id": 1}', $receivedByB);

        // Both groups have subscription rows
        $stmt = $this->pdo->query('SELECT consumer_group FROM domain_event_subscriptions ORDER BY consumer_group');
        $groups = $stmt->fetchAll(PDO::FETCH_COLUMN);

        $this->assertCount(2, $groups);
        $this->assertSame('email-service', $groups[0]);
        $this->assertSame('pdf-service', $groups[1]);
    }

    public function testConsumeWithGroupDoesNotReprocessAfterAck(): void
    {
        $this->insertEvent('InvoiceCreated', '{"id": 1}');
        $this->insertEvent('InvoiceCreated', '{"id": 2}');

        $options = new DatabaseTransportOptions(pollingIntervalMs: 0);

        // First run: process event 1
        $consumer = new DatabaseConsumer($this->connection, $options);
        $consumer->consume('InvoiceCreated', function () use ($consumer): bool {
            $consumer->stop();
            return true;
        }, ['group' => 'email-service']);

        // Second run: should get event 2 (event 1 already processed)
        $consumer2 = new DatabaseConsumer($this->connection, $options);
        $received = null;
        $consumer2->consume('InvoiceCreated', function (string $message) use (&$received, $consumer2): bool {
            $received = $message;
            $consumer2->stop();
            return true;
        }, ['group' => 'email-service']);

        $this->assertSame('{"id": 2}', $received);
    }

    public function testConsumeWithGroupIncrementsAttempts(): void
    {
        $this->insertEvent('InvoiceCreated', '{"id": 1}');

        $options = new DatabaseTransportOptions(pollingIntervalMs: 0, maxAttempts: 3);
        $consumer = new DatabaseConsumer($this->connection, $options);

        $consumer->consume('InvoiceCreated', function (): bool {
            return false;
        }, ['group' => 'email-service']);

        $stmt = $this->pdo->query(
            'SELECT attempts, last_error FROM domain_event_subscriptions WHERE consumer_group = \'email-service\''
        );
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertSame(1, (int) $row['attempts']);
        $this->assertSame('Handler returned false', $row['last_error']);
    }

    public function testConsumeWithGroupSkipsExhaustedAttempts(): void
    {
        $this->insertEvent('InvoiceCreated', '{"id": 1}');
        $this->insertEvent('InvoiceCreated', '{"id": 2}');

        // Pre-insert exhausted subscription for event 1
        $stmt = $this->pdo->prepare(
            "INSERT INTO domain_event_subscriptions (event_id, consumer_group, attempts, last_error) VALUES (?, ?, ?, ?)"
        );
        $stmt->execute([1, 'email-service', 3, 'Previous error']);

        $options = new DatabaseTransportOptions(pollingIntervalMs: 0, maxAttempts: 3);
        $consumer = new DatabaseConsumer($this->connection, $options);

        $messages = [];
        $consumer->consume('InvoiceCreated', function (string $message) use (&$messages, $consumer): bool {
            $messages[] = $message;
            $consumer->stop();
            return true;
        }, ['group' => 'email-service']);

        $this->assertCount(1, $messages);
        $this->assertSame('{"id": 2}', $messages[0]);
    }

    public function testConsumeWithGroupPassesGroupInMetadata(): void
    {
        $this->insertEvent('InvoiceCreated', '{"id": 1}');

        $options = new DatabaseTransportOptions(pollingIntervalMs: 0);
        $consumer = new DatabaseConsumer($this->connection, $options);

        $receivedMetadata = [];
        $consumer->consume('InvoiceCreated', function (string $message, array $metadata) use (&$receivedMetadata, $consumer): bool {
            $receivedMetadata = $metadata;
            $consumer->stop();
            return true;
        }, ['group' => 'audit-service']);

        $this->assertSame('audit-service', $receivedMetadata['group']);
        $this->assertSame('database', $receivedMetadata['transport']);
        $this->assertSame(1, $receivedMetadata['id']);
    }

    public function testConsumeWithoutGroupDoesNotAffectSubscriptions(): void
    {
        $this->insertEvent('InvoiceCreated', '{"id": 1}');

        $options = new DatabaseTransportOptions(pollingIntervalMs: 0);

        // Point-to-Point consume
        $consumer = new DatabaseConsumer($this->connection, $options);
        $consumer->consume('InvoiceCreated', function () use ($consumer): bool {
            $consumer->stop();
            return true;
        });

        // No subscription rows should exist
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM domain_event_subscriptions');
        $this->assertSame(0, (int) $stmt->fetchColumn());

        // domain_events.processed_at should be set
        $stmt = $this->pdo->query('SELECT processed_at FROM domain_events WHERE id = 1');
        $this->assertNotNull($stmt->fetchColumn());
    }

    public function testConsumeWithGroupDoesNotTouchEventsTable(): void
    {
        $this->insertEvent('InvoiceCreated', '{"id": 1}');

        $options = new DatabaseTransportOptions(pollingIntervalMs: 0);
        $consumer = new DatabaseConsumer($this->connection, $options);

        $consumer->consume('InvoiceCreated', function () use ($consumer): bool {
            $consumer->stop();
            return true;
        }, ['group' => 'email-service']);

        // domain_events.processed_at should remain NULL (fan-out uses subscription table)
        $stmt = $this->pdo->query('SELECT processed_at FROM domain_events WHERE id = 1');
        $this->assertNull($stmt->fetchColumn());
    }

    public function testConsumeWithGroupHandlesException(): void
    {
        $this->insertEvent('InvoiceCreated', '{"id": 1}');

        $options = new DatabaseTransportOptions(pollingIntervalMs: 0, maxAttempts: 2);
        $consumer = new DatabaseConsumer($this->connection, $options);

        try {
            $consumer->consume('InvoiceCreated', function (): bool {
                throw new \RuntimeException('Service unavailable');
            }, ['group' => 'email-service']);
            $this->fail('Expected RuntimeException to be thrown');
        } catch (\RuntimeException $e) {
            $this->assertSame('Service unavailable', $e->getMessage());
        }

        $stmt = $this->pdo->query(
            'SELECT attempts, last_error FROM domain_event_subscriptions WHERE consumer_group = \'email-service\''
        );
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertSame(1, (int) $row['attempts']);
        $this->assertSame('Service unavailable', $row['last_error']);
    }

    public function testConsumePointToPointMetadataHasNoGroup(): void
    {
        $this->insertEvent('OrderCreated', '{"id": 1}');

        $options = new DatabaseTransportOptions(pollingIntervalMs: 0);
        $consumer = new DatabaseConsumer($this->connection, $options);

        $receivedMetadata = [];
        $consumer->consume('OrderCreated', function (string $message, array $metadata) use (&$receivedMetadata, $consumer): bool {
            $receivedMetadata = $metadata;
            $consumer->stop();
            return true;
        });

        $this->assertArrayNotHasKey('group', $receivedMetadata);
    }

    // -------------------------------------------------------------------------
    // PDOException Handling in consume()-Loop
    // -------------------------------------------------------------------------

    public function testConsumePdoExceptionWhileRunningThrowsConsumerException(): void
    {
        $options = new DatabaseTransportOptions(pollingIntervalMs: 0);
        $consumer = new DatabaseConsumer($this->connection, $options);

        // Drop the table so the first fetchUnprocessed() call throws a PDOException
        $this->pdo->exec('DROP TABLE domain_events');

        $this->expectException(\JardisSupport\Contract\Messaging\Exception\ConsumerException::class);
        $this->expectExceptionMessageMatches('/Failed to consume from database/');

        $consumer->consume('OrderCreated', function (): bool {
            return true;
        });
    }

    public function testConsumePdoExceptionAfterStopIsSuppressed(): void
    {
        $this->insertEvent('OrderCreated', '{"id": 1}');

        $options = new DatabaseTransportOptions(pollingIntervalMs: 0);
        $consumer = new DatabaseConsumer($this->connection, $options);

        // Process one event successfully, then stop and drop the table.
        // The next loop iteration calls fetchUnprocessed() on the dropped table.
        // Because running = false after stop(), the PDOException must be suppressed.
        $consumer->consume('OrderCreated', function () use ($consumer): bool {
            $consumer->stop();
            $this->pdo->exec('DROP TABLE domain_events');
            return true;
        });

        // If we reach this point the exception was suppressed — assert nothing was thrown
        $this->addToAssertionCount(1);
    }

    // -------------------------------------------------------------------------
    // Inner-catch: incrementAttemptsForGroup fails → original exception re-thrown
    // -------------------------------------------------------------------------

    public function testConsumeWithGroupIncrementAttemptsFailureStillRethrowsOriginalException(): void
    {
        $this->insertEvent('InvoiceCreated', '{"id": 1}');

        $options = new DatabaseTransportOptions(pollingIntervalMs: 0, maxAttempts: 2);
        $consumer = new DatabaseConsumer($this->connection, $options);

        // Drop the subscription table inside the callback — after the event is fetched
        // but before incrementAttemptsForGroup() is called in the catch-block.
        // The inner catch(\Throwable) silences the PDOException from the broken table,
        // and the original RuntimeException must still propagate.
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('callback error');

        $consumer->consume('InvoiceCreated', function (): bool {
            $this->pdo->exec('DROP TABLE domain_event_subscriptions');
            throw new \RuntimeException('callback error');
        }, ['group' => 'email-service']);
    }

    // -------------------------------------------------------------------------
    // Inner-catch: incrementAttempts (Point-to-Point) fails → original exception re-thrown
    // -------------------------------------------------------------------------

    public function testConsumeIncrementAttemptsFailureStillRethrowsOriginalException(): void
    {
        $this->insertEvent('OrderCreated', '{"id": 1}');

        $options = new DatabaseTransportOptions(pollingIntervalMs: 0, maxAttempts: 2);
        $consumer = new DatabaseConsumer($this->connection, $options);

        // Rename / break the events table after the event is fetched so that
        // the UPDATE in incrementAttempts() fails, triggering the inner catch.
        $called = false;
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('point-to-point callback error');

        $consumer->consume('OrderCreated', function () use (&$called): bool {
            if (!$called) {
                $called = true;
                // Break the table so the subsequent UPDATE in incrementAttempts() throws
                $this->pdo->exec('ALTER TABLE domain_events RENAME TO domain_events_broken');
            }
            throw new \RuntimeException('point-to-point callback error');
        });
    }

    private function insertEvent(string $topic, string $payload, int $attempts = 0): void
    {
        $stmt = $this->pdo->prepare(
            "INSERT INTO domain_events (topic, payload, created_at, attempts) VALUES (?, ?, datetime('now'), ?)"
        );
        $stmt->execute([$topic, $payload, $attempts]);
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

    private function createSubscriptionTable(PDO $pdo, string $table = 'domain_event_subscriptions'): void
    {
        $pdo->exec(
            "CREATE TABLE {$table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                consumer_group VARCHAR(255) NOT NULL,
                processed_at TEXT NULL DEFAULT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NULL DEFAULT NULL,
                UNIQUE(event_id, consumer_group)
            )"
        );
    }
}
