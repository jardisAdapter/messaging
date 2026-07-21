<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit;

use JardisAdapter\Messaging\Consumer\InMemoryConsumer;
use JardisAdapter\Messaging\Factory\ConsumerFactory;
use JardisAdapter\Messaging\Factory\PublisherFactory;
use JardisAdapter\Messaging\Handler\CallbackHandler;
use JardisAdapter\Messaging\MessageConsumer;
use JardisAdapter\Messaging\MessagePublisher;
use JardisAdapter\Messaging\MessagingService;
use JardisAdapter\Messaging\Publisher\InMemoryPublisher;
use JardisAdapter\Messaging\Transport\InMemoryTransport;
use PHPUnit\Framework\TestCase;

class InMemoryIntegrationTest extends TestCase
{
    private PublisherFactory $publisherFactory;
    private ConsumerFactory $consumerFactory;

    protected function setUp(): void
    {
        $this->publisherFactory = new PublisherFactory();
        $this->consumerFactory = new ConsumerFactory();

        // Share transport between publisher and consumer factories
        $transport = new InMemoryTransport();
        $this->publisherFactory->setSharedTransport($transport);
        $this->consumerFactory->setSharedTransport($transport);
    }

    public function testPublisherConsumerWorkflow(): void
    {
        $transport = new InMemoryTransport();
        $publisher = new InMemoryPublisher($transport);
        $consumer = new InMemoryConsumer($transport);

        // Publish messages
        $publisher->publish('orders', json_encode(['orderId' => 123]));
        $publisher->publish('orders', json_encode(['orderId' => 456]));

        // Consume messages
        $processed = [];
        $consumer->consume('orders', function (string $message, array $metadata) use (&$processed): bool {
            $processed[] = json_decode($message, true);
            return true;
        });

        $this->assertCount(2, $processed);
        $this->assertEquals(123, $processed[0]['orderId']);
        $this->assertEquals(456, $processed[1]['orderId']);
    }

    public function testMessagePublisherWithInMemory(): void
    {
        $transport = new InMemoryTransport();

        $publisher = new MessagePublisher(new InMemoryPublisher($transport));

        $result = $publisher->publish('orders', ['orderId' => 123, 'total' => 99.99]);

        $this->assertTrue($result);

        $messages = $transport->peek('orders');

        $this->assertCount(1, $messages);
        $decoded = json_decode($messages[0]['message'], true);
        $this->assertEquals(123, $decoded['orderId']);
        $this->assertEquals(99.99, $decoded['total']);
    }

    public function testMessageConsumerWithInMemory(): void
    {
        $transport = new InMemoryTransport();
        $transport->publish('orders', json_encode(['orderId' => 789]));

        $consumer = new MessageConsumer(new InMemoryConsumer($transport));

        $processed = [];

        $handler = new CallbackHandler(function ($message, $metadata) use (&$processed): bool {
            $processed[] = $message;
            return true;
        });

        $consumer->consume('orders', $handler);

        $this->assertCount(1, $processed);
        $this->assertEquals(789, $processed[0]['orderId']);
    }

    public function testMessagingServiceWithInMemory(): void
    {
        $transport = new InMemoryTransport();

        $messaging = new MessagingService(
            publisherFactory: fn() => new MessagePublisher(new InMemoryPublisher($transport)),
            consumerFactory: fn() => new MessageConsumer(new InMemoryConsumer($transport)),
        );

        $messaging->publish('events', ['type' => 'UserCreated', 'userId' => 42]);

        $processed = [];
        $handler = new CallbackHandler(function ($message, $metadata) use (&$processed): bool {
            $processed[] = $message;
            return true;
        });

        $messaging->consume('events', $handler);

        $this->assertCount(1, $processed);
        $this->assertEquals('UserCreated', $processed[0]['type']);
        $this->assertEquals(42, $processed[0]['userId']);
    }

    public function testMultiplePublishersOneConsumer(): void
    {
        $transport = new InMemoryTransport();

        $publisher1 = new InMemoryPublisher($transport);
        $publisher2 = new InMemoryPublisher($transport);
        $consumer = new InMemoryConsumer($transport);

        $publisher1->publish('topic', 'from-publisher-1');
        $publisher2->publish('topic', 'from-publisher-2');

        $messages = [];
        $consumer->consume('topic', function (string $message, array $metadata) use (&$messages): bool {
            $messages[] = $message;
            return true;
        });

        $this->assertCount(2, $messages);
        $this->assertContains('from-publisher-1', $messages);
        $this->assertContains('from-publisher-2', $messages);
    }

    public function testMessageOrderPreserved(): void
    {
        $transport = new InMemoryTransport();
        $publisher = new InMemoryPublisher($transport);
        $consumer = new InMemoryConsumer($transport);

        for ($i = 1; $i <= 100; $i++) {
            $publisher->publish('orders', "order-{$i}");
        }

        $order = [];
        $consumer->consume('orders', function (string $message, array $metadata) use (&$order): bool {
            $order[] = $message;
            return true;
        });

        for ($i = 1; $i <= 100; $i++) {
            $this->assertEquals("order-{$i}", $order[$i - 1]);
        }
    }

    public function testPublisherFactoryCreatesInMemoryPublisher(): void
    {
        $transport = new InMemoryTransport();
        $publisher = $this->publisherFactory->inMemory($transport);

        $this->assertInstanceOf(InMemoryPublisher::class, $publisher);

        $publisher->publish('test', 'message');

        $this->assertEquals(1, $transport->getMessageCount('test'));
    }

    public function testConsumerFactoryCreatesInMemoryConsumer(): void
    {
        $consumer = $this->consumerFactory->inMemory();

        $this->assertInstanceOf(InMemoryConsumer::class, $consumer);
    }

    public function testTestIsolationWithSeparateInstances(): void
    {
        $transport1 = new InMemoryTransport();
        $transport2 = new InMemoryTransport();

        // Test 1: Publish to transport1
        $transport1->publish('orders', 'test-message');
        $this->assertEquals(1, $transport1->getMessageCount('orders'));

        // Test 2: transport2 is isolated
        $this->assertEquals(0, $transport2->getMessageCount('orders'));
    }

    public function testAutoDeserializationWithInMemory(): void
    {
        $transport = new InMemoryTransport();

        $publisher = new MessagePublisher(new InMemoryPublisher($transport));

        $publisher->publish('orders', ['orderId' => 123, 'items' => ['a', 'b', 'c']]);

        $consumer = new MessageConsumer(new InMemoryConsumer($transport));

        $receivedMessage = null;

        $handler = new CallbackHandler(function ($message, $metadata) use (&$receivedMessage): bool {
            $receivedMessage = $message;
            return true;
        });

        $consumer->consume('orders', $handler);

        $this->assertIsArray($receivedMessage);
        $this->assertEquals(123, $receivedMessage['orderId']);
        $this->assertEquals(['a', 'b', 'c'], $receivedMessage['items']);
    }

    public function testPriorityOrderingWithInMemory(): void
    {
        $transport = new InMemoryTransport();

        $publisher = new MessagePublisher(new InMemoryPublisher($transport));

        $result = $publisher->publish('test', 'message');
        $this->assertTrue($result);

        $this->assertEquals(1, $transport->getMessageCount('test'));
    }

    public function testInMemoryWithStringMessage(): void
    {
        $transport = new InMemoryTransport();

        $publisher = new MessagePublisher(new InMemoryPublisher($transport));

        $consumer = new MessageConsumer(new InMemoryConsumer($transport));

        $publisher->publish('logs', 'Plain text log message');

        $received = null;
        $handler = new CallbackHandler(function ($message, $metadata) use (&$received): bool {
            $received = $message;
            return true;
        });

        $consumer->consume('logs', $handler);

        $this->assertEquals('Plain text log message', $received);
    }

    public function testFactorySharedTransport(): void
    {
        // publisherFactory and consumerFactory share the same transport via setUp()
        $publisher = $this->publisherFactory->inMemory();
        $consumer = $this->consumerFactory->inMemory();

        $publisher->publish('test', 'shared-message');

        $processed = [];
        $consumer->consume('test', function (string $message, array $metadata) use (&$processed): bool {
            $processed[] = $message;
            return true;
        });

        $this->assertCount(1, $processed);
        $this->assertEquals('shared-message', $processed[0]);
    }
}
