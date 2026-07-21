<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Integration\Consumer;

use JardisAdapter\Messaging\Config\ConnectionConfig;
use JardisAdapter\Messaging\Connection\RedisConnection;
use JardisAdapter\Messaging\Consumer\RedisConsumer;
use JardisAdapter\Messaging\Publisher\RedisPublisher;
use PHPUnit\Framework\TestCase;

class RedisConsumerTest extends TestCase
{
    private ConnectionConfig $config;
    private RedisConnection $connection;

    protected function setUp(): void
    {
        $this->config = new ConnectionConfig(
            host: $_ENV['REDIS_HOST'] ?? 'redis',
            port: 6379
        );

        $this->connection = new RedisConnection($this->config);
    }

    protected function tearDown(): void
    {
        $this->connection->disconnect();
    }


    public function testCreatesConsumer(): void
    {
        $consumer = new RedisConsumer($this->connection, useStreams: false);
        $this->assertInstanceOf(RedisConsumer::class, $consumer);

        $streamConsumer = new RedisConsumer($this->connection, useStreams: true);
        $this->assertInstanceOf(RedisConsumer::class, $streamConsumer);
    }

    public function testStopsConsuming(): void
    {
        $consumer = new RedisConsumer($this->connection, useStreams: false);
        $consumer->stop();
        $this->assertTrue(true); // No exception thrown
    }

    public function testConsumesStreamMessage(): void
    {
        // Publish messages first
        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $publisher->publish('test.consume.stream', 'Message 1');
        $publisher->publish('test.consume.stream', 'Message 2');

        $consumer = new RedisConsumer($this->connection, useStreams: true);
        $received = [];

        // Consume with auto-stop after 2 messages
        $consumer->consume('test.consume.stream', function (string $message, array $meta) use (&$received): bool {
            $received[] = $message;
            // Stop after receiving 2 messages
            return count($received) < 2;
        }, ['start_id' => '0', 'block' => 100]);

        $this->assertCount(2, $received);
        $this->assertSame('Message 1', $received[0]);
        $this->assertSame('Message 2', $received[1]);

        // Cleanup
        $this->connection->connect();
        $this->connection->getClient()->del('test.consume.stream');
    }

    public function testConsumesStreamWithMetadata(): void
    {
        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $publisher->publish('metadata.stream', 'test message');

        $consumer = new RedisConsumer($this->connection, useStreams: true);
        $receivedMeta = null;

        $consumer->consume('metadata.stream', function (string $message, array $meta) use (&$receivedMeta): bool {
            $receivedMeta = $meta;
            return false; // Stop after first
        }, ['start_id' => '0', 'block' => 100]);

        $this->assertNotNull($receivedMeta);
        $this->assertArrayHasKey('id', $receivedMeta);
        $this->assertArrayHasKey('stream', $receivedMeta);
        $this->assertArrayHasKey('type', $receivedMeta);
        $this->assertSame('stream', $receivedMeta['type']);
        $this->assertSame('metadata.stream', $receivedMeta['stream']);

        // Cleanup
        $this->connection->connect();
        $this->connection->getClient()->del('metadata.stream');
    }

    public function testPublishAndConsumeStream(): void
    {
        // Publish messages
        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $publisher->publish('integration.stream', json_encode(['id' => 1, 'action' => 'test']));

        // Read back immediately (no blocking consumer)
        $this->connection->connect();
        $redis = $this->connection->getClient();

        $messages = $redis->xRange('integration.stream', '-', '+');

        $this->assertNotEmpty($messages);
        $firstMessage = reset($messages);
        $this->assertArrayHasKey('message', $firstMessage);

        $decoded = json_decode($firstMessage['message'], true);
        $this->assertSame(1, $decoded['id']);
        $this->assertSame('test', $decoded['action']);

        // Cleanup
        $redis->del('integration.stream');
    }

    public function testStreamWithCustomFields(): void
    {
        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $publisher->publish('custom.stream', 'test message', [
            'fields' => ['user_id' => 123, 'type' => 'event']
        ]);

        $this->connection->connect();
        $redis = $this->connection->getClient();

        $messages = $redis->xRange('custom.stream', '-', '+');
        $this->assertNotEmpty($messages);

        $firstMessage = reset($messages);
        $this->assertSame('test message', $firstMessage['message']);
        $this->assertSame('123', $firstMessage['user_id']);
        $this->assertSame('event', $firstMessage['type']);

        // Cleanup
        $redis->del('custom.stream');
    }

    public function testAutoConnectsIfNotConnected(): void
    {
        $consumer = new RedisConsumer($this->connection, useStreams: true);

        $this->assertFalse($this->connection->isConnected());

        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $publisher->publish('autoconnect.stream', 'test');

        $received = null;
        $consumer->consume('autoconnect.stream', function (string $message, array $meta) use (&$received): bool {
            $received = $message;
            return false;
        }, ['start_id' => '0', 'block' => 100]);

        $this->assertNotNull($received);
        $this->assertTrue($this->connection->isConnected());

        // Cleanup
        $this->connection->connect();
        $this->connection->getClient()->del('autoconnect.stream');
    }

    public function testStopDuringConsumption(): void
    {
        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $publisher->publish('stop.stream', 'Message 1');
        $publisher->publish('stop.stream', 'Message 2');
        $publisher->publish('stop.stream', 'Message 3');

        $consumer = new RedisConsumer($this->connection, useStreams: true);
        $received = [];

        $consumer->consume('stop.stream', function (string $message, array $meta) use (&$received): bool {
            $received[] = $message;
            return false; // Stop immediately
        }, ['start_id' => '0', 'block' => 100]);

        $this->assertCount(1, $received);

        // Cleanup
        $this->connection->connect();
        $this->connection->getClient()->del('stop.stream');
    }

    public function testConsumerGroupSupport(): void
    {
        // Test that consumer group creation doesn't fail
        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $publisher->publish('group.stream', 'Message for group');

        $consumer = new RedisConsumer($this->connection, useStreams: true);
        $received = null;

        $consumer->consume('group.stream', function (string $message, array $meta) use (&$received): bool {
            $received = $message;
            return false;
        }, [
            'group' => 'test-group',
            'consumer' => 'consumer-1',
            'block' => 100,
            'count' => 1
        ]);

        $this->assertNotNull($received);
        $this->assertSame('Message for group', $received);

        // Cleanup
        $this->connection->connect();
        $redis = $this->connection->getClient();
        try {
            $redis->xGroup('DESTROY', 'group.stream', 'test-group');
        } catch (\RedisException $e) {
            // Ignore if group doesn't exist
        }
        $redis->del('group.stream');
    }

    public function testStreamReadsFromLastId(): void
    {
        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $publisher->publish('lastid.stream', 'Message 1');
        $publisher->publish('lastid.stream', 'Message 2');
        $publisher->publish('lastid.stream', 'Message 3');

        // Get the ID of the first message
        $this->connection->connect();
        $redis = $this->connection->getClient();
        $messages = $redis->xRange('lastid.stream', '-', '+', 1);
        $firstId = array_key_first($messages);

        $consumer = new RedisConsumer($this->connection, useStreams: true);
        $received = [];

        // Start reading from after first message
        $consumer->consume('lastid.stream', function (string $message, array $meta) use (&$received): bool {
            $received[] = $message;
            return count($received) < 2;
        }, ['start_id' => $firstId, 'block' => 100]);

        $this->assertCount(2, $received);
        $this->assertSame('Message 2', $received[0]);
        $this->assertSame('Message 3', $received[1]);

        // Cleanup
        $redis->del('lastid.stream');
    }

    // -------------------------------------------------------------------------
    // Stream Consumer Group Edge Cases
    // -------------------------------------------------------------------------

    public function testConsumerGroupAckVerification(): void
    {
        $stream = 'test.group.ack.' . uniqid();
        $group  = 'ack-group';
        $consumerName = 'ack-consumer';

        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $publisher->publish($stream, 'ack this message');

        $consumer = new RedisConsumer($this->connection, useStreams: true);
        $received = null;

        $consumer->consume(
            $stream,
            function (string $message, array $meta) use (&$received, $consumer): bool {
                $received = $message;
                // ACK the message (return true) then stop the consumer
                $consumer->stop();
                return true;
            },
            [
                'group'    => $group,
                'consumer' => $consumerName,
                'block'    => 100,
                'count'    => 1,
            ]
        );

        $this->assertSame('ack this message', $received);

        // PEL muss leer sein — Message wurde ACKed
        $this->connection->connect();
        $redis = $this->connection->getClient();
        $pending = $redis->xPending($stream, $group, '-', '+', 10);
        $this->assertEmpty($pending, 'PEL should be empty after ACK');

        // Cleanup
        try {
            $redis->xGroup('DESTROY', $stream, $group);
        } catch (\RedisException) {
        }
        $redis->del($stream);
    }

    public function testConsumerGroupCallbackFalseNoAck(): void
    {
        $stream = 'test.group.noack.' . uniqid();
        $group  = 'noack-group';
        $consumerName = 'noack-consumer';

        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $publisher->publish($stream, 'do not ack');

        $consumer = new RedisConsumer($this->connection, useStreams: true);

        $consumer->consume(
            $stream,
            function (string $message, array $meta): bool {
                return false; // kein ACK
            },
            [
                'group'    => $group,
                'consumer' => $consumerName,
                'block'    => 100,
                'count'    => 1,
            ]
        );

        // Message muss in PEL stehen
        $this->connection->connect();
        $redis = $this->connection->getClient();
        $pending = $redis->xPending($stream, $group, '-', '+', 10);
        $this->assertNotEmpty($pending, 'PEL should contain unacked message');

        // Cleanup
        try {
            $redis->xGroup('DESTROY', $stream, $group);
        } catch (\RedisException) {
        }
        $redis->del($stream);
    }

    public function testStreamCallbackExceptionPropagates(): void
    {
        $stream = 'test.stream.exc.' . uniqid();

        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $publisher->publish($stream, 'trigger exception');

        $consumer = new RedisConsumer($this->connection, useStreams: true);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('stream callback error');

        try {
            $consumer->consume(
                $stream,
                function (string $message, array $meta): bool {
                    throw new \RuntimeException('stream callback error');
                },
                ['start_id' => '0', 'block' => 100]
            );
        } finally {
            $this->connection->connect();
            $this->connection->getClient()->del($stream);
        }
    }

    public function testStreamWithMissingMessageField(): void
    {
        $stream = 'test.stream.nomsg.' . uniqid();

        // Direkt via xAdd ohne 'message'-Key publizieren
        $this->connection->connect();
        $redis = $this->connection->getClient();
        $redis->xAdd($stream, '*', ['event' => 'OrderCreated', 'id' => '42']);

        $consumer = new RedisConsumer($this->connection, useStreams: true);
        $received = null;

        $consumer->consume(
            $stream,
            function (string $message, array $meta) use (&$received): bool {
                $received = $message;
                return false;
            },
            ['start_id' => '0', 'block' => 100]
        );

        $this->assertNotNull($received);

        // Kein 'message'-Key → Consumer enkodiert alle Felder als JSON-Fallback
        $decoded = json_decode($received, true);
        $this->assertIsArray($decoded);
        $this->assertSame('OrderCreated', $decoded['event']);
        $this->assertSame('42', $decoded['id']);

        // Cleanup
        $redis->del($stream);
    }
}
