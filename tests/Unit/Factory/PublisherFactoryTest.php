<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit\Factory;

use JardisAdapter\Messaging\Connection\ExternalDatabaseConnection;
use JardisAdapter\Messaging\Factory\PublisherFactory;
use JardisAdapter\Messaging\Publisher\DatabasePublisher;
use JardisAdapter\Messaging\Publisher\InMemoryPublisher;
use JardisAdapter\Messaging\Transport\InMemoryTransport;
use JardisSupport\Contract\Messaging\PublisherInterface;
use PDO;
use PHPUnit\Framework\TestCase;

class PublisherFactoryTest extends TestCase
{
    private PublisherFactory $factory;

    protected function setUp(): void
    {
        $this->factory = new PublisherFactory();
    }

    public function testInMemoryReturnsPublisherInterface(): void
    {
        $publisher = $this->factory->inMemory();

        $this->assertInstanceOf(PublisherInterface::class, $publisher);
        $this->assertInstanceOf(InMemoryPublisher::class, $publisher);
    }

    public function testInMemoryWithCustomTransport(): void
    {
        $transport = new InMemoryTransport();
        $publisher = $this->factory->inMemory($transport);

        $publisher->publish('test', 'message');

        $this->assertEquals(1, $transport->getMessageCount('test'));
    }

    public function testInMemoryUsesSharedTransport(): void
    {
        $transport = new InMemoryTransport();
        $this->factory->setSharedTransport($transport);

        $publisher = $this->factory->inMemory();
        $publisher->publish('test', 'message');

        $this->assertEquals(1, $transport->getMessageCount('test'));
    }

    public function testDatabaseReturnsDatabasePublisher(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $connection = new ExternalDatabaseConnection($pdo);

        $publisher = $this->factory->database($connection);

        $this->assertInstanceOf(DatabasePublisher::class, $publisher);
    }

    public function testGetSharedTransportCreatesSingleInstance(): void
    {
        $transport1 = $this->factory->getSharedTransport();
        $transport2 = $this->factory->getSharedTransport();

        $this->assertSame($transport1, $transport2);
    }
}
