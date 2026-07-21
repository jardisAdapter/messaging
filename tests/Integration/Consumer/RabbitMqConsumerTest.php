<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Integration\Consumer;

use JardisAdapter\Messaging\Config\ConnectionConfig;
use JardisAdapter\Messaging\Connection\RabbitMqConnection;
use JardisAdapter\Messaging\Consumer\RabbitMqConsumer;
use JardisAdapter\Messaging\Publisher\RabbitMqPublisher;
use PHPUnit\Framework\TestCase;

class RabbitMqConsumerTest extends TestCase
{
    private ConnectionConfig $config;
    private RabbitMqConnection $connection;

    protected function setUp(): void
    {
        $this->config = new ConnectionConfig(
            host: $_ENV['RABBITMQ_HOST'] ?? 'rabbitmq',
            port: (int)($_ENV['RABBITMQ_PORT'] ?? 5672),
            username: $_ENV['RABBITMQ_USER'] ?? 'guest',
            password: $_ENV['RABBITMQ_PASSWORD'] ?? 'guest'
        );

        $this->connection = new RabbitMqConnection($this->config);
    }

    protected function tearDown(): void
    {
        $this->connection->disconnect();
    }

    public function testCreatesConsumer(): void
    {
        $consumer = new RabbitMqConsumer($this->connection, 'test_queue');

        $this->assertInstanceOf(RabbitMqConsumer::class, $consumer);
    }

    public function testStopsConsuming(): void
    {
        $consumer = new RabbitMqConsumer($this->connection, 'test_queue');

        $consumer->stop();

        $this->assertTrue(true); // No exception thrown
    }

    public function testPublishAndSetupQueue(): void
    {
        // Publish a message
        $publisher = new RabbitMqPublisher($this->connection);
        $result = $publisher->publish('test.routing.key', json_encode(['test' => 'message']));

        $this->assertTrue($result);
    }

    public function testConsumerWithCallback(): void
    {
        $uniqueSuffix = uniqid('', true);
        $queueName = 'test_consume_' . $uniqueSuffix;
        $routingKey = 'test.consume.' . $uniqueSuffix;

        // 1. Setup: Queue deklarieren + an Exchange binden
        $consumer = new RabbitMqConsumer($this->connection, $queueName);
        $consumer->consume($routingKey, function (): bool { return true; }, [
            'timeout' => 0.01,
            'max_empty_polls' => 1,
        ]);

        // 2. Publish (Queue existiert + ist gebunden)
        $publisher = new RabbitMqPublisher($this->connection);
        $publisher->publish($routingKey, 'hello from test', ['routing_key' => $routingKey]);

        // 3. Consume
        $received = [];
        $consumer->consume(
            $routingKey,
            function (string $message, array $meta) use (&$received, $consumer): bool {
                $received[] = $message;
                $consumer->stop();
                return true;
            },
            ['timeout' => 0.5, 'max_empty_polls' => 10]
        );

        $this->assertCount(1, $received);
        $this->assertSame('hello from test', $received[0]);

        $this->deleteQueue($queueName);
    }

    public function testConsumerReceivesMetadata(): void
    {
        $uniqueSuffix = uniqid('', true);
        $queueName = 'test_meta_' . $uniqueSuffix;
        $routingKey = 'test.meta.' . $uniqueSuffix;

        // 1. Setup: Queue deklarieren + an Exchange binden
        $consumer = new RabbitMqConsumer($this->connection, $queueName);
        $consumer->consume($routingKey, function (): bool { return true; }, [
            'timeout' => 0.01,
            'max_empty_polls' => 1,
        ]);

        // 2. Publish
        $publisher = new RabbitMqPublisher($this->connection);
        $publisher->publish($routingKey, 'metadata test', ['routing_key' => $routingKey]);

        // 3. Consume + Metadata erfassen
        $capturedMeta = [];
        $consumer->consume(
            $routingKey,
            function (string $message, array $meta) use (&$capturedMeta, $consumer): bool {
                $capturedMeta = $meta;
                $consumer->stop();
                return true;
            },
            ['timeout' => 0.5, 'max_empty_polls' => 10]
        );

        $this->assertNotEmpty($capturedMeta, 'Metadata should not be empty');
        $this->assertArrayHasKey('routing_key', $capturedMeta);
        $this->assertArrayHasKey('delivery_tag', $capturedMeta);
        $this->assertArrayHasKey('exchange', $capturedMeta);
        $this->assertSame('rabbitmq', $capturedMeta['type']);
        $this->assertSame($routingKey, $capturedMeta['routing_key']);

        $this->deleteQueue($queueName);
    }

    public function testMultiplePublishOperations(): void
    {
        $publisher = new RabbitMqPublisher($this->connection);

        $results = [];
        for ($i = 1; $i <= 10; $i++) {
            $results[] = $publisher->publish("test.key.{$i}", json_encode(['msg' => $i]));
        }

        $this->assertCount(10, $results);
        $this->assertCount(10, array_filter($results)); // All should be true
    }

    public function testPublishWithPriority(): void
    {
        $publisher = new RabbitMqPublisher($this->connection);

        $result = $publisher->publish('test.priority', 'high priority message', [
            'attributes' => [
                'priority' => 9,
                'delivery_mode' => 2
            ]
        ]);

        $this->assertTrue($result);
    }

    public function testConsumerHandlesTimeout(): void
    {
        $uniqueSuffix = time() . '-' . rand(1000, 9999);
        $queueName = 'timeout_queue_' . $uniqueSuffix;
        $routingKey = "timeout.key.{$uniqueSuffix}";

        // Don't publish anything - test timeout behavior
        $consumer = new RabbitMqConsumer($this->connection, $queueName);
        $messageCount = 0;

        $consumer->consume(
            $routingKey,
            function (string $message, array $meta) use (&$messageCount): bool {
                $messageCount++;
                return true;
            },
            ['timeout' => 0.05, 'max_empty_polls' => 3]
        );

        // No messages should be received, consumer should timeout
        $this->assertSame(0, $messageCount);

        $this->deleteQueue($queueName);
    }

    public function testConsumerStopsOnCallbackReturnFalse(): void
    {
        $uniqueSuffix = uniqid('', true);
        $queueName = 'test_stop_' . $uniqueSuffix;
        $routingKey = 'test.stop.' . $uniqueSuffix;

        // 1. Setup: Queue deklarieren + an Exchange binden
        $consumer = new RabbitMqConsumer($this->connection, $queueName);
        $consumer->consume($routingKey, function (): bool { return true; }, [
            'timeout' => 0.01,
            'max_empty_polls' => 1,
        ]);

        // 2. Drei Messages publishen
        $publisher = new RabbitMqPublisher($this->connection);
        $publisher->publish($routingKey, 'message 1', ['routing_key' => $routingKey]);
        $publisher->publish($routingKey, 'message 2', ['routing_key' => $routingKey]);
        $publisher->publish($routingKey, 'message 3', ['routing_key' => $routingKey]);

        // 3. Consume: callback gibt bei 1. Message false zurück → Consumer stoppt + NACK+REQUEUE
        $received = [];
        $consumer->consume(
            $routingKey,
            function (string $message, array $meta) use (&$received): bool {
                $received[] = $message;
                return false; // Stop after first message — triggers NACK + REQUEUE
            },
            ['timeout' => 0.5, 'max_empty_polls' => 10]
        );

        // Nur 1 Message empfangen, dann gestoppt
        $this->assertCount(1, $received);
        $this->assertSame('message 1', $received[0]);

        // 4. Verify REQUEUE: Message 1 ist wieder in der Queue (NACK+REQUEUE)
        $requeued = [];
        $consumer->consume(
            $routingKey,
            function (string $message, array $meta) use (&$requeued, $consumer): bool {
                $requeued[] = $message;
                $consumer->stop();
                return true; // ACK
            },
            ['timeout' => 0.5, 'max_empty_polls' => 10]
        );

        $this->assertNotEmpty($requeued, 'NACKed message should be requeued and re-consumable');
        $this->assertSame('message 1', $requeued[0]);

        $this->deleteQueue($queueName);
    }

    public function testConsumerCallbackExceptionPropagatesAndNacks(): void
    {
        $uniqueSuffix = uniqid('', true);
        $queueName = 'test_exception_' . $uniqueSuffix;
        $routingKey = 'test.exception.' . $uniqueSuffix;

        // 1. Setup: Queue deklarieren + an Exchange binden
        $consumer = new RabbitMqConsumer($this->connection, $queueName);
        $consumer->consume($routingKey, function (): bool { return true; }, [
            'timeout' => 0.01,
            'max_empty_polls' => 1,
        ]);

        // 2. Publish
        $publisher = new RabbitMqPublisher($this->connection);
        $publisher->publish($routingKey, 'exception test', ['routing_key' => $routingKey]);

        // 3. Consume mit Exception im Callback — Exception muss propagiert werden
        $exceptionThrown = null;
        try {
            $consumer->consume(
                $routingKey,
                function (string $message, array $meta): bool {
                    throw new \RuntimeException('Callback error');
                },
                ['timeout' => 0.5, 'max_empty_polls' => 10]
            );
        } catch (\RuntimeException $e) {
            $exceptionThrown = $e;
        }

        $this->assertNotNull($exceptionThrown, 'Exception from callback should propagate');
        $this->assertSame('Callback error', $exceptionThrown->getMessage());

        // 4. Verify REQUEUE: Message wurde NACK+REQUEUE'd → kann nochmal consumed werden
        $requeued = [];
        $consumer->consume(
            $routingKey,
            function (string $message, array $meta) use (&$requeued, $consumer): bool {
                $requeued[] = $message;
                $consumer->stop();
                return true; // ACK
            },
            ['timeout' => 0.5, 'max_empty_polls' => 10]
        );

        $this->assertNotEmpty($requeued, 'Exception-NACKed message should be requeued');
        $this->assertSame('exception test', $requeued[0]);

        $this->deleteQueue($queueName);
    }

    public function testConsumerWithQueueConfigApplied(): void
    {
        $uniqueSuffix = uniqid('', true);
        $queueName = 'test_config_' . $uniqueSuffix;
        $routingKey = 'test.config.' . $uniqueSuffix;

        $queueConfig = [
            'flags' => AMQP_DURABLE,
            'arguments' => ['x-message-ttl' => 60000],
        ];

        // Queue mit Config deklarieren — kein Exception erwartet
        $consumer = new RabbitMqConsumer($this->connection, $queueName, $queueConfig);
        $consumer->consume($routingKey, function (): bool { return true; }, [
            'timeout' => 0.01,
            'max_empty_polls' => 1,
        ]);

        $this->assertTrue(true); // Queue wurde ohne Exception erstellt

        $this->deleteQueue($queueName);
    }

    /**
     * Hilfsmethode: Queue in RabbitMQ löschen
     */
    private function deleteQueue(string $queueName): void
    {
        try {
            if (!$this->connection->isConnected()) {
                $this->connection->connect();
            }
            $channel = $this->connection->getChannel();
            $queue = new \AMQPQueue($channel);
            $queue->setName($queueName);
            $queue->delete();
        } catch (\Throwable) {
            // Ignore cleanup errors
        }
    }
}
