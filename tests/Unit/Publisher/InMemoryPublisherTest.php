<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit\Publisher;

use JardisAdapter\Messaging\Publisher\InMemoryPublisher;
use JardisAdapter\Messaging\Transport\InMemoryTransport;
use PHPUnit\Framework\TestCase;

class InMemoryPublisherTest extends TestCase
{
    public function testPublishStoresMessage(): void
    {
        $transport = new InMemoryTransport();
        $publisher = new InMemoryPublisher($transport);

        $result = $publisher->publish('orders', 'test message');

        $this->assertTrue($result);

        $messages = $transport->peek('orders');

        $this->assertCount(1, $messages);
        $this->assertEquals('test message', $messages[0]['message']);
    }

    public function testPublishReturnsTrue(): void
    {
        $transport = new InMemoryTransport();
        $publisher = new InMemoryPublisher($transport);

        $result = $publisher->publish('orders', 'message');

        $this->assertTrue($result);
    }

    public function testPublishWithMetadata(): void
    {
        $transport = new InMemoryTransport();
        $publisher = new InMemoryPublisher($transport);

        $publisher->publish('orders', 'message', [
            'metadata' => ['priority' => 'high', 'source' => 'api']
        ]);

        $messages = $transport->peek('orders');

        $this->assertEquals(['priority' => 'high', 'source' => 'api'], $messages[0]['metadata']);
    }

    public function testPublishToMultipleTopics(): void
    {
        $transport = new InMemoryTransport();
        $publisher = new InMemoryPublisher($transport);

        $publisher->publish('orders', 'order message');
        $publisher->publish('users', 'user message');
        $publisher->publish('events', 'event message');

        $this->assertEquals(1, $transport->getMessageCount('orders'));
        $this->assertEquals(1, $transport->getMessageCount('users'));
        $this->assertEquals(1, $transport->getMessageCount('events'));
    }

    public function testPublishMultipleMessagesToSameTopic(): void
    {
        $transport = new InMemoryTransport();
        $publisher = new InMemoryPublisher($transport);

        $publisher->publish('orders', 'message1');
        $publisher->publish('orders', 'message2');
        $publisher->publish('orders', 'message3');

        $this->assertEquals(3, $transport->getMessageCount('orders'));
    }

    public function testPublishWithCustomTransport(): void
    {
        $transport = new InMemoryTransport();
        $publisher = new InMemoryPublisher($transport);

        $publisher->publish('orders', 'message');

        $this->assertEquals(1, $transport->getMessageCount('orders'));
    }

    public function testPublishJsonPayload(): void
    {
        $transport = new InMemoryTransport();
        $publisher = new InMemoryPublisher($transport);

        $jsonPayload = json_encode(['order_id' => 123, 'total' => 99.99]);
        $publisher->publish('orders', $jsonPayload);

        $messages = $transport->peek('orders');

        $this->assertEquals($jsonPayload, $messages[0]['message']);
    }

    public function testPublishEmptyMessage(): void
    {
        $transport = new InMemoryTransport();
        $publisher = new InMemoryPublisher($transport);

        $result = $publisher->publish('orders', '');

        $this->assertTrue($result);

        $messages = $transport->peek('orders');

        $this->assertEquals('', $messages[0]['message']);
    }

    public function testPublishWithEmptyMetadataOption(): void
    {
        $transport = new InMemoryTransport();
        $publisher = new InMemoryPublisher($transport);

        $publisher->publish('orders', 'message', ['metadata' => []]);

        $messages = $transport->peek('orders');

        $this->assertEquals([], $messages[0]['metadata']);
    }

    public function testPublishWithNonArrayMetadataIsIgnored(): void
    {
        $transport = new InMemoryTransport();
        $publisher = new InMemoryPublisher($transport);

        $publisher->publish('orders', 'message', ['metadata' => 'not-an-array']);

        $messages = $transport->peek('orders');

        $this->assertEquals([], $messages[0]['metadata']);
    }

    public function testPublishWithOtherOptionsDoesNotAffectMetadata(): void
    {
        $transport = new InMemoryTransport();
        $publisher = new InMemoryPublisher($transport);

        $publisher->publish('orders', 'message', [
            'some_option' => 'value',
            'another' => 123
        ]);

        $messages = $transport->peek('orders');

        $this->assertEquals([], $messages[0]['metadata']);
    }

    public function testGetConnectionReturnsSameInstance(): void
    {
        $publisher = new InMemoryPublisher();

        $conn1 = $publisher->getConnection();
        $conn2 = $publisher->getConnection();

        $this->assertSame($conn1, $conn2);
    }
}
