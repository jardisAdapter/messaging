<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Integration\Publisher;

use JardisAdapter\Messaging\Config\ConnectionConfig;
use JardisAdapter\Messaging\Connection\RabbitMqConnection;
use JardisAdapter\Messaging\Publisher\RabbitMqPublisher;
use PHPUnit\Framework\TestCase;

class RabbitMqPublisherTest extends TestCase
{
    private RabbitMqConnection $connection;
    private ConnectionConfig $config;

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

    public function testPublishesMessage(): void
    {
        $publisher = new RabbitMqPublisher($this->connection);

        $result = $publisher->publish('test.routing.key', 'test message');

        $this->assertTrue($result);
    }

    public function testPublishesMultipleMessages(): void
    {
        $publisher = new RabbitMqPublisher($this->connection);

        $result1 = $publisher->publish('test.routing.key', 'message 1');
        $result2 = $publisher->publish('test.routing.key', 'message 2');
        $result3 = $publisher->publish('test.routing.key', 'message 3');

        $this->assertTrue($result1);
        $this->assertTrue($result2);
        $this->assertTrue($result3);
    }

    public function testPublishesToDifferentRoutingKeys(): void
    {
        $publisher = new RabbitMqPublisher($this->connection);

        $result1 = $publisher->publish('orders.created', 'order created');
        $result2 = $publisher->publish('orders.updated', 'order updated');
        $result3 = $publisher->publish('orders.deleted', 'order deleted');

        $this->assertTrue($result1);
        $this->assertTrue($result2);
        $this->assertTrue($result3);
    }

    public function testPublishesWithCustomRoutingKey(): void
    {
        $publisher = new RabbitMqPublisher($this->connection);

        $result = $publisher->publish(
            'default.key',
            'test message',
            ['routing_key' => 'custom.routing.key']
        );

        $this->assertTrue($result);
    }

    public function testPublishesWithCustomFlags(): void
    {
        $publisher = new RabbitMqPublisher($this->connection);

        $result = $publisher->publish(
            'test.key',
            'test message',
            ['flags' => AMQP_MANDATORY]
        );

        $this->assertTrue($result);
    }

    public function testPublishesWithCustomAttributes(): void
    {
        $publisher = new RabbitMqPublisher($this->connection);

        $result = $publisher->publish(
            'test.key',
            'test message',
            [
                'attributes' => [
                    'content_type' => 'text/plain',
                    'delivery_mode' => 1, // Non-persistent
                    'priority' => 5,
                    'timestamp' => time()
                ]
            ]
        );

        $this->assertTrue($result);
    }

    public function testPublishesWithAllOptions(): void
    {
        $publisher = new RabbitMqPublisher($this->connection);

        $result = $publisher->publish(
            'default.key',
            'test message',
            [
                'routing_key' => 'custom.key',
                'flags' => AMQP_MANDATORY,
                'attributes' => [
                    'content_type' => 'application/json',
                    'priority' => 9,
                    'app_id' => 'test-app'
                ]
            ]
        );

        $this->assertTrue($result);
    }

    public function testPublishesJsonData(): void
    {
        $publisher = new RabbitMqPublisher($this->connection);

        $jsonMessage = json_encode([
            'event' => 'user.registered',
            'user_id' => 123,
            'email' => 'test@example.com',
            'timestamp' => time()
        ]);

        $result = $publisher->publish('user.events', $jsonMessage);

        $this->assertTrue($result);
    }

    public function testAutoConnectsOnFirstPublish(): void
    {
        $publisher = new RabbitMqPublisher($this->connection);

        $this->assertFalse($this->connection->isConnected());

        $result = $publisher->publish('test.key', 'test message');

        $this->assertTrue($result);
        $this->assertTrue($this->connection->isConnected());
    }

    public function testPublishesWithExistingConnection(): void
    {
        $this->connection->connect();
        $publisher = new RabbitMqPublisher($this->connection);

        $result = $publisher->publish('test.key', 'test message');

        $this->assertTrue($result);
    }

    public function testPublishesLargeMessage(): void
    {
        $publisher = new RabbitMqPublisher($this->connection);

        $largeMessage = str_repeat('x', 10000);

        $result = $publisher->publish('test.key', $largeMessage);

        $this->assertTrue($result);
    }

    public function testPublishesBatchOfMessages(): void
    {
        $publisher = new RabbitMqPublisher($this->connection);

        $messages = [];
        for ($i = 1; $i <= 100; $i++) {
            $result = $publisher->publish('batch.key', "message {$i}");
            $messages[] = $result;
        }

        // All should succeed
        $this->assertCount(100, array_filter($messages));
    }

    public function testPublishesWithEmptyMessage(): void
    {
        $publisher = new RabbitMqPublisher($this->connection);

        $result = $publisher->publish('test.key', '');

        $this->assertTrue($result);
    }

    public function testPublishesWithDifferentDeliveryModes(): void
    {
        $publisher = new RabbitMqPublisher($this->connection);

        // Non-persistent
        $result1 = $publisher->publish(
            'test.key',
            'non-persistent',
            ['attributes' => ['delivery_mode' => 1]]
        );

        // Persistent (default)
        $result2 = $publisher->publish(
            'test.key',
            'persistent',
            ['attributes' => ['delivery_mode' => 2]]
        );

        $this->assertTrue($result1);
        $this->assertTrue($result2);
    }

    public function testPublishesWithWildcardRoutingKey(): void
    {
        $publisher = new RabbitMqPublisher($this->connection);

        $result1 = $publisher->publish('logs.*.error', 'error log');
        $result2 = $publisher->publish('logs.#', 'all logs');

        $this->assertTrue($result1);
        $this->assertTrue($result2);
    }

    public function testPublishesWithDefaultAttributes(): void
    {
        // No custom attributes — delivery_mode=2 and content_type=application/json should be applied
        $publisher = new RabbitMqPublisher($this->connection);

        $result = $publisher->publish('test.key', 'test message');

        $this->assertTrue($result);
    }

    public function testPublishesWithMergedAttributes(): void
    {
        // Custom attributes are merged with defaults — expiration and app_id added,
        // content_type overrides the default value
        $publisher = new RabbitMqPublisher($this->connection);

        $result = $publisher->publish('test.key', 'test message', [
            'attributes' => [
                'content_type' => 'text/plain',
                'expiration' => '60000',
                'app_id' => 'test-app',
            ],
        ]);

        $this->assertTrue($result);
    }

    public function testPublishesWithNoparamFlag(): void
    {
        // Explicitly pass AMQP_NOPARAM (the default) — verifies the flags path is exercised
        $publisher = new RabbitMqPublisher($this->connection);

        $result = $publisher->publish('test.key', 'test message', [
            'flags' => AMQP_NOPARAM,
        ]);

        $this->assertTrue($result);
    }
}
