<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit\Factory;

use JardisAdapter\Messaging\Connection\ExternalDatabaseConnection;
use JardisAdapter\Messaging\Consumer\DatabaseConsumer;
use JardisAdapter\Messaging\Consumer\InMemoryConsumer;
use JardisAdapter\Messaging\Factory\ConsumerFactory;
use JardisAdapter\Messaging\Transport\InMemoryTransport;
use JardisSupport\Contract\Messaging\ConsumerInterface;
use PDO;
use PHPUnit\Framework\TestCase;

class ConsumerFactoryTest extends TestCase
{
    private ConsumerFactory $factory;

    protected function setUp(): void
    {
        $this->factory = new ConsumerFactory();
    }

    public function testInMemoryReturnsConsumerInterface(): void
    {
        $consumer = $this->factory->inMemory();

        $this->assertInstanceOf(ConsumerInterface::class, $consumer);
        $this->assertInstanceOf(InMemoryConsumer::class, $consumer);
    }

    public function testInMemoryWithCustomTransport(): void
    {
        $transport = new InMemoryTransport();
        $transport->publish('test', 'message');

        $consumer = $this->factory->inMemory($transport);
        $processed = [];

        $consumer->consume('test', function (string $message, array $metadata) use (&$processed): bool {
            $processed[] = $message;
            return true;
        });

        $this->assertCount(1, $processed);
        $this->assertEquals('message', $processed[0]);
    }

    public function testInMemoryUsesSharedTransport(): void
    {
        $transport = new InMemoryTransport();
        $transport->publish('test', 'message');
        $this->factory->setSharedTransport($transport);

        $consumer = $this->factory->inMemory();
        $processed = [];

        $consumer->consume('test', function (string $message, array $metadata) use (&$processed): bool {
            $processed[] = $message;
            return true;
        });

        $this->assertCount(1, $processed);
    }

    public function testDatabaseReturnsDatabaseConsumer(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $connection = new ExternalDatabaseConnection($pdo);

        $consumer = $this->factory->database($connection);

        $this->assertInstanceOf(DatabaseConsumer::class, $consumer);
    }

    public function testGetSharedTransportCreatesSingleInstance(): void
    {
        $transport1 = $this->factory->getSharedTransport();
        $transport2 = $this->factory->getSharedTransport();

        $this->assertSame($transport1, $transport2);
    }
}
