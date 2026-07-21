<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit\Consumer;

use JardisAdapter\Messaging\Consumer\InMemoryConsumer;
use JardisAdapter\Messaging\Transport\InMemoryTransport;
use PHPUnit\Framework\TestCase;

class InMemoryConsumerTest extends TestCase
{
    public function testConsumeProcessesAllMessages(): void
    {
        $transport = new InMemoryTransport();
        $transport->publish('orders', 'message1');
        $transport->publish('orders', 'message2');
        $transport->publish('orders', 'message3');

        $consumer = new InMemoryConsumer($transport);
        $processed = [];

        $consumer->consume('orders', function (string $message, array $metadata) use (&$processed): bool {
            $processed[] = $message;
            return true;
        });

        $this->assertCount(3, $processed);
        $this->assertEquals(['message1', 'message2', 'message3'], $processed);
    }

    public function testConsumeRemovesProcessedMessages(): void
    {
        $transport = new InMemoryTransport();
        $transport->publish('orders', 'message1');
        $transport->publish('orders', 'message2');

        $consumer = new InMemoryConsumer($transport);

        $consumer->consume('orders', function (string $message, array $metadata): bool {
            return true;
        });

        $this->assertEquals(0, $transport->getMessageCount('orders'));
    }

    public function testConsumeCallsCallbackForEachMessage(): void
    {
        $transport = new InMemoryTransport();
        $transport->publish('orders', 'message1');
        $transport->publish('orders', 'message2');

        $consumer = new InMemoryConsumer($transport);
        $callCount = 0;

        $consumer->consume('orders', function (string $message, array $metadata) use (&$callCount): bool {
            $callCount++;
            return true;
        });

        $this->assertEquals(2, $callCount);
    }

    public function testConsumeWithLimit(): void
    {
        $transport = new InMemoryTransport();
        $transport->publish('orders', 'message1');
        $transport->publish('orders', 'message2');
        $transport->publish('orders', 'message3');
        $transport->publish('orders', 'message4');

        $consumer = new InMemoryConsumer($transport);
        $processed = [];

        $consumer->consume('orders', function (string $message, array $metadata) use (&$processed): bool {
            $processed[] = $message;
            return true;
        }, ['limit' => 2]);

        $this->assertCount(2, $processed);
        $this->assertEquals(['message1', 'message2'], $processed);

        // Remaining messages should still be in queue
        $this->assertEquals(2, $transport->getMessageCount('orders'));
    }

    public function testConsumeWithPeek(): void
    {
        $transport = new InMemoryTransport();
        $transport->publish('orders', 'message1');
        $transport->publish('orders', 'message2');

        $consumer = new InMemoryConsumer($transport);
        $processed = [];

        $consumer->consume('orders', function (string $message, array $metadata) use (&$processed): bool {
            $processed[] = $message;
            return true;
        }, ['peek' => true]);

        $this->assertCount(2, $processed);

        // Messages should still be in queue (peek mode)
        $this->assertEquals(2, $transport->getMessageCount('orders'));
    }

    public function testCallbackReceivesCorrectMetadata(): void
    {
        $transport = new InMemoryTransport();
        $transport->publish('orders', 'message', ['custom' => 'value']);

        $consumer = new InMemoryConsumer($transport);
        $receivedMetadata = null;

        $consumer->consume('orders', function (string $message, array $metadata) use (&$receivedMetadata): bool {
            $receivedMetadata = $metadata;
            return true;
        });

        $this->assertArrayHasKey('topic', $receivedMetadata);
        $this->assertArrayHasKey('timestamp', $receivedMetadata);
        $this->assertArrayHasKey('type', $receivedMetadata);
        $this->assertArrayHasKey('index', $receivedMetadata);
        $this->assertArrayHasKey('custom', $receivedMetadata);

        $this->assertEquals('orders', $receivedMetadata['topic']);
        $this->assertEquals('inmemory', $receivedMetadata['type']);
        $this->assertEquals(0, $receivedMetadata['index']);
        $this->assertEquals('value', $receivedMetadata['custom']);
    }

    public function testStopIsNoOp(): void
    {
        $consumer = new InMemoryConsumer();

        // Should not throw any exception
        $consumer->stop();

        $this->assertTrue(true);
    }

    public function testConsumeEmptyTopic(): void
    {
        $consumer = new InMemoryConsumer();
        $callCount = 0;

        $consumer->consume('empty-topic', function (string $message, array $metadata) use (&$callCount): bool {
            $callCount++;
            return true;
        });

        $this->assertEquals(0, $callCount);
    }

    public function testConsumeWithCustomTransport(): void
    {
        $transport = new InMemoryTransport();
        $transport->publish('orders', 'message');

        $consumer = new InMemoryConsumer($transport);
        $processed = [];

        $consumer->consume('orders', function (string $message, array $metadata) use (&$processed): bool {
            $processed[] = $message;
            return true;
        });

        $this->assertCount(1, $processed);
    }

    public function testMetadataIndexIncrementsForEachMessage(): void
    {
        $transport = new InMemoryTransport();
        $transport->publish('orders', 'message1');
        $transport->publish('orders', 'message2');
        $transport->publish('orders', 'message3');

        $consumer = new InMemoryConsumer($transport);
        $indices = [];

        $consumer->consume('orders', function (string $message, array $metadata) use (&$indices): bool {
            $indices[] = $metadata['index'];
            return true;
        });

        $this->assertEquals([0, 1, 2], $indices);
    }

    public function testConsumeWithLimitAndPeek(): void
    {
        $transport = new InMemoryTransport();
        $transport->publish('orders', 'message1');
        $transport->publish('orders', 'message2');
        $transport->publish('orders', 'message3');

        $consumer = new InMemoryConsumer($transport);
        $processed = [];

        $consumer->consume('orders', function (string $message, array $metadata) use (&$processed): bool {
            $processed[] = $message;
            return true;
        }, ['limit' => 2, 'peek' => true]);

        $this->assertCount(2, $processed);
        // With peek, all messages remain
        $this->assertEquals(3, $transport->getMessageCount('orders'));
    }

    public function testConsumePreservesMessageOrder(): void
    {
        $transport = new InMemoryTransport();

        for ($i = 1; $i <= 5; $i++) {
            $transport->publish('orders', "message{$i}");
        }

        $consumer = new InMemoryConsumer($transport);
        $order = [];

        $consumer->consume('orders', function (string $message, array $metadata) use (&$order): bool {
            $order[] = $message;
            return true;
        });

        $this->assertEquals(['message1', 'message2', 'message3', 'message4', 'message5'], $order);
    }

    public function testCallbackFalseStopsConsumption(): void
    {
        $transport = new InMemoryTransport();
        $transport->publish('orders', 'message1');
        $transport->publish('orders', 'message2');
        $transport->publish('orders', 'message3');

        $consumer = new InMemoryConsumer($transport);
        $processed = [];

        // Return false on first message — should stop and re-queue current + remaining
        $consumer->consume('orders', function (string $message, array $metadata) use (&$processed): bool {
            $processed[] = $message;
            return false;
        });

        $this->assertCount(1, $processed);
        $this->assertEquals(['message1'], $processed);

        // Current (rejected) + remaining messages should be re-queued
        $this->assertEquals(3, $transport->getMessageCount('orders'));
    }

    public function testCallbackFalseOnSecondMessageRequeuesOnlyRemaining(): void
    {
        $transport = new InMemoryTransport();
        $transport->publish('orders', 'message1');
        $transport->publish('orders', 'message2');
        $transport->publish('orders', 'message3');

        $consumer = new InMemoryConsumer($transport);
        $processed = [];

        $consumer->consume('orders', function (string $message, array $metadata) use (&$processed): bool {
            $processed[] = $message;
            return $message !== 'message2'; // false on second
        });

        $this->assertEquals(['message1', 'message2'], $processed);
        $this->assertEquals(2, $transport->getMessageCount('orders'));

        // message2 (rejected) and message3 should be re-queued
        $remaining = $transport->peek('orders');
        $this->assertEquals('message2', $remaining[0]['message']);
        $this->assertEquals('message3', $remaining[1]['message']);
    }

    public function testCallbackFalsePreservesTimestamps(): void
    {
        $transport = new InMemoryTransport();
        $transport->publish('orders', 'message1');
        $transport->publish('orders', 'message2');

        // Peek to capture original timestamps
        $originalMessages = $transport->peek('orders');
        $originalTimestamp = $originalMessages[0]['timestamp'];

        $consumer = new InMemoryConsumer($transport);

        // Stop after first message (rejected)
        $consumer->consume('orders', function (string $message, array $metadata): bool {
            return false;
        });

        // Both messages re-queued; rejected message1 should preserve original timestamp
        $remaining = $transport->peek('orders');
        $this->assertCount(2, $remaining);
        $this->assertEquals($originalTimestamp, $remaining[0]['timestamp']);
    }

    public function testCallbackExceptionRequeuesCurrentAndRemainingMessages(): void
    {
        $transport = new InMemoryTransport();
        $transport->publish('orders', 'message1');
        $transport->publish('orders', 'message2');
        $transport->publish('orders', 'message3');
        $transport->publish('orders', 'message4');
        $transport->publish('orders', 'message5');

        $consumer = new InMemoryConsumer($transport);
        $processed = [];

        try {
            $consumer->consume('orders', function (string $message, array $metadata) use (&$processed): bool {
                $processed[] = $message;
                if ($message === 'message2') {
                    throw new \RuntimeException('Processing failed');
                }
                return true;
            });
            $this->fail('Expected RuntimeException was not thrown');
        } catch (\RuntimeException $e) {
            $this->assertEquals('Processing failed', $e->getMessage());
        }

        // message1 processed successfully, message2 threw exception
        $this->assertEquals(['message1', 'message2'], $processed);

        // message2 (current) + message3-5 (remaining) should be re-queued
        $this->assertEquals(4, $transport->getMessageCount('orders'));

        $remaining = $transport->peek('orders');
        $this->assertEquals('message2', $remaining[0]['message']);
        $this->assertEquals('message3', $remaining[1]['message']);
        $this->assertEquals('message4', $remaining[2]['message']);
        $this->assertEquals('message5', $remaining[3]['message']);
    }

    public function testCallbackExceptionInPeekModeDoesNotRequeue(): void
    {
        $transport = new InMemoryTransport();
        $transport->publish('orders', 'message1');
        $transport->publish('orders', 'message2');
        $transport->publish('orders', 'message3');

        $consumer = new InMemoryConsumer($transport);

        try {
            $consumer->consume('orders', function (string $message, array $metadata): bool {
                if ($message === 'message1') {
                    throw new \RuntimeException('Processing failed');
                }
                return true;
            }, ['peek' => true]);
            $this->fail('Expected RuntimeException was not thrown');
        } catch (\RuntimeException $e) {
            $this->assertEquals('Processing failed', $e->getMessage());
        }

        // Peek mode: original messages untouched, no requeue
        $this->assertEquals(3, $transport->getMessageCount('orders'));
    }

    public function testGetConnectionReturnsSameInstance(): void
    {
        $consumer = new InMemoryConsumer();

        $conn1 = $consumer->getConnection();
        $conn2 = $consumer->getConnection();

        $this->assertSame($conn1, $conn2);
    }
}
