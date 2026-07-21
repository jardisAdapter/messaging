<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Integration;

use JardisAdapter\Messaging\Factory\ConnectionFactory;
use JardisAdapter\Messaging\Factory\ConsumerFactory;
use JardisAdapter\Messaging\Factory\PublisherFactory;
use JardisAdapter\Messaging\Handler\CallbackHandler;
use JardisAdapter\Messaging\MessageConsumer;
use JardisAdapter\Messaging\MessagePublisher;
use JardisAdapter\Messaging\MessagingService;
use PHPUnit\Framework\TestCase;

class MessagingServiceTest extends TestCase
{
    private ConnectionFactory $connectionFactory;
    private PublisherFactory $publisherFactory;
    private ConsumerFactory $consumerFactory;

    protected function setUp(): void
    {
        $this->connectionFactory = new ConnectionFactory();
        $this->publisherFactory = new PublisherFactory();
        $this->consumerFactory = new ConsumerFactory();
    }

    private function createRedisPublisher(): MessagePublisher
    {
        $redisConn = $this->connectionFactory->redis($_ENV['REDIS_HOST'] ?? 'redis');
        return new MessagePublisher(
            $this->publisherFactory->redis($redisConn, useStreams: true),
        );
    }

    private function createRedisConsumer(): MessageConsumer
    {
        $redisConn = $this->connectionFactory->redis($_ENV['REDIS_HOST'] ?? 'redis');
        return new MessageConsumer(
            $this->consumerFactory->redis($redisConn, useStreams: true),
        );
    }

    public function testPublishWithRedis(): void
    {
        $service = new MessagingService(
            publisherFactory: fn() => $this->createRedisPublisher(),
            consumerFactory: fn() => $this->createRedisConsumer()
        );

        $result = $service->publish('test.service.channel', 'Test message from MessagingService');

        $this->assertTrue($result);
    }

    public function testPublishArrayWithRedis(): void
    {
        $service = new MessagingService(
            publisherFactory: fn() => $this->createRedisPublisher(),
            consumerFactory: fn() => $this->createRedisConsumer()
        );

        $result = $service->publish('test.service.array', [
            'type' => 'order',
            'id' => 12345,
            'amount' => 99.99
        ]);

        $this->assertTrue($result);
    }

    public function testPublishWithFallback(): void
    {
        $service = new MessagingService(
            publisherFactory: fn() => $this->createRedisPublisher(),
            consumerFactory: fn() => $this->createRedisConsumer()
        );

        $result = $service->publish('test.service.fallback', 'Fallback test message');

        $this->assertTrue($result);
    }

    public function testGetPublisher(): void
    {
        $service = new MessagingService(
            publisherFactory: fn() => $this->createRedisPublisher(),
            consumerFactory: fn() => $this->createRedisConsumer()
        );

        $retrievedPublisher = $service->getPublisher();

        $this->assertInstanceOf(MessagePublisher::class, $retrievedPublisher);
    }

    public function testGetConsumer(): void
    {
        $service = new MessagingService(
            publisherFactory: fn() => $this->createRedisPublisher(),
            consumerFactory: fn() => $this->createRedisConsumer()
        );

        $retrievedConsumer = $service->getConsumer();

        $this->assertInstanceOf(MessageConsumer::class, $retrievedConsumer);
    }

    public function testLazyLoadingPublisher(): void
    {
        $publisherCreated = false;

        $connFactory = $this->connectionFactory;
        $pFactory = $this->publisherFactory;
        $service = new MessagingService(
            publisherFactory: function () use (&$publisherCreated, $connFactory, $pFactory) {
                $publisherCreated = true;
                $redisConn = $connFactory->redis($_ENV['REDIS_HOST'] ?? 'redis');
                return new MessagePublisher(
                    $pFactory->redis($redisConn),
                );
            },
            consumerFactory: fn() => $this->createRedisConsumer()
        );

        $this->assertFalse($publisherCreated);

        $service->publish('test.lazy.publisher', 'test');

        $this->assertTrue($publisherCreated);
    }

    public function testLazyLoadingConsumer(): void
    {
        $consumerCreated = false;

        $connFactory = $this->connectionFactory;
        $cFactory = $this->consumerFactory;
        $service = new MessagingService(
            publisherFactory: fn() => $this->createRedisPublisher(),
            consumerFactory: function () use (&$consumerCreated, $connFactory, $cFactory) {
                $consumerCreated = true;
                $redisConn = $connFactory->redis($_ENV['REDIS_HOST'] ?? 'redis');
                return new MessageConsumer(
                    $cFactory->redis($redisConn),
                );
            }
        );

        $this->assertFalse($consumerCreated);

        $service->getConsumer();

        $this->assertTrue($consumerCreated);
    }

    public function testPublishAndConsumeRoundTrip(): void
    {
        $service = new MessagingService(
            publisherFactory: fn() => $this->createRedisPublisher(),
            consumerFactory: fn() => $this->createRedisConsumer()
        );

        $testMessage = ['user' => 'test', 'action' => 'roundtrip', 'timestamp' => time()];
        $service->publish('test.service.roundtrip', $testMessage);

        $receivedMessage = null;
        $handler = new CallbackHandler(function ($message) use (&$receivedMessage, $service) {
            $receivedMessage = $message;
            $service->getConsumer()->stop();
            return true;
        });

        $service->consume('test.service.roundtrip', $handler, [
            'group' => 'test-roundtrip-group',
            'consumer' => 'test-consumer'
        ]);

        $this->assertNotNull($receivedMessage);
        $this->assertIsArray($receivedMessage);
        $this->assertEquals('test', $receivedMessage['user']);
        $this->assertEquals('roundtrip', $receivedMessage['action']);
    }
}
