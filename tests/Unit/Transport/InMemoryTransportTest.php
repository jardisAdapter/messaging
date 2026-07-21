<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit\Transport;

use JardisAdapter\Messaging\Transport\InMemoryTransport;
use PHPUnit\Framework\TestCase;

class InMemoryTransportTest extends TestCase
{
    public function testPublishAndConsume(): void
    {
        $transport = new InMemoryTransport();

        $transport->publish('orders', 'message1');
        $transport->publish('orders', 'message2');

        $messages = $transport->consume('orders');

        $this->assertCount(2, $messages);
        $this->assertEquals('message1', $messages[0]['message']);
        $this->assertEquals('message2', $messages[1]['message']);
    }

    public function testConsumeRemovesMessages(): void
    {
        $transport = new InMemoryTransport();

        $transport->publish('orders', 'message1');
        $transport->consume('orders');

        $this->assertEquals(0, $transport->getMessageCount('orders'));
    }

    public function testPeekDoesNotRemoveMessages(): void
    {
        $transport = new InMemoryTransport();

        $transport->publish('orders', 'message1');
        $transport->publish('orders', 'message2');

        $peeked = $transport->peek('orders');
        $this->assertCount(2, $peeked);

        // Messages should still be there
        $this->assertEquals(2, $transport->getMessageCount('orders'));
    }

    public function testGetMessageCount(): void
    {
        $transport = new InMemoryTransport();

        $this->assertEquals(0, $transport->getMessageCount('orders'));

        $transport->publish('orders', 'message1');
        $this->assertEquals(1, $transport->getMessageCount('orders'));

        $transport->publish('orders', 'message2');
        $this->assertEquals(2, $transport->getMessageCount('orders'));
    }

    public function testClearSpecificTopic(): void
    {
        $transport = new InMemoryTransport();

        $transport->publish('orders', 'order1');
        $transport->publish('users', 'user1');

        $transport->clear('orders');

        $this->assertEquals(0, $transport->getMessageCount('orders'));
        $this->assertEquals(1, $transport->getMessageCount('users'));
    }

    public function testClearAllTopics(): void
    {
        $transport = new InMemoryTransport();

        $transport->publish('orders', 'order1');
        $transport->publish('users', 'user1');

        $transport->clear();

        $this->assertEquals(0, $transport->getMessageCount('orders'));
        $this->assertEquals(0, $transport->getMessageCount('users'));
    }

    public function testMultipleTopicsAreIsolated(): void
    {
        $transport = new InMemoryTransport();

        $transport->publish('orders', 'order1');
        $transport->publish('users', 'user1');
        $transport->publish('users', 'user2');

        $this->assertEquals(1, $transport->getMessageCount('orders'));
        $this->assertEquals(2, $transport->getMessageCount('users'));

        $orderMessages = $transport->consume('orders');
        $userMessages = $transport->consume('users');

        $this->assertCount(1, $orderMessages);
        $this->assertCount(2, $userMessages);
        $this->assertEquals('order1', $orderMessages[0]['message']);
    }

    public function testMessageTimestamp(): void
    {
        $transport = new InMemoryTransport();

        $before = microtime(true);
        $transport->publish('orders', 'message');
        $after = microtime(true);

        $messages = $transport->peek('orders');

        $this->assertGreaterThanOrEqual($before, $messages[0]['timestamp']);
        $this->assertLessThanOrEqual($after, $messages[0]['timestamp']);
    }

    public function testPublishWithMetadata(): void
    {
        $transport = new InMemoryTransport();

        $transport->publish('orders', 'message', ['priority' => 'high', 'source' => 'api']);

        $messages = $transport->peek('orders');

        $this->assertEquals(['priority' => 'high', 'source' => 'api'], $messages[0]['metadata']);
    }

    public function testConsumeEmptyTopic(): void
    {
        $transport = new InMemoryTransport();

        $messages = $transport->consume('nonexistent');

        $this->assertIsArray($messages);
        $this->assertEmpty($messages);
    }

    public function testPeekEmptyTopic(): void
    {
        $transport = new InMemoryTransport();

        $messages = $transport->peek('nonexistent');

        $this->assertIsArray($messages);
        $this->assertEmpty($messages);
    }

    public function testGetTopics(): void
    {
        $transport = new InMemoryTransport();

        $transport->publish('orders', 'order1');
        $transport->publish('users', 'user1');
        $transport->publish('events', 'event1');

        $topics = $transport->getTopics();

        $this->assertCount(3, $topics);
        $this->assertContains('orders', $topics);
        $this->assertContains('users', $topics);
        $this->assertContains('events', $topics);
    }

    public function testGetTopicsEmpty(): void
    {
        $transport = new InMemoryTransport();

        $topics = $transport->getTopics();

        $this->assertIsArray($topics);
        $this->assertEmpty($topics);
    }

    public function testMessageOrderIsPreserved(): void
    {
        $transport = new InMemoryTransport();

        for ($i = 1; $i <= 10; $i++) {
            $transport->publish('orders', "message{$i}");
        }

        $messages = $transport->consume('orders');

        for ($i = 0; $i < 10; $i++) {
            $this->assertEquals("message" . ($i + 1), $messages[$i]['message']);
        }
    }

    public function testSeparateInstancesAreIsolated(): void
    {
        $transport1 = new InMemoryTransport();
        $transport2 = new InMemoryTransport();

        $transport1->publish('topic', 'message1');

        $this->assertEquals(1, $transport1->getMessageCount('topic'));
        $this->assertEquals(0, $transport2->getMessageCount('topic'));
    }
}
