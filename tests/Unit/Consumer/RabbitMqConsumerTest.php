<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit\Consumer;

use AMQPEnvelope;
use AMQPQueue;
use JardisAdapter\Messaging\Config\ConnectionConfig;
use JardisAdapter\Messaging\Connection\RabbitMqConnection;
use JardisAdapter\Messaging\Consumer\RabbitMqConsumer;
use JardisSupport\Contract\Messaging\Exception\ConsumerException;
use PHPUnit\Framework\TestCase;

class RabbitMqConsumerTest extends TestCase
{
    public function testConstructor(): void
    {
        $config = new ConnectionConfig(
            host: 'localhost',
            port: 5672,
            username: 'guest',
            password: 'guest'
        );
        $connection = $this->createMock(RabbitMqConnection::class);

        $consumer = new RabbitMqConsumer($connection, 'test_queue');

        $this->assertInstanceOf(RabbitMqConsumer::class, $consumer);
    }

    public function testConstructorWithQueueConfig(): void
    {
        $connection = $this->createMock(RabbitMqConnection::class);
        $queueConfig = [
            'flags' => AMQP_DURABLE,
            'arguments' => ['x-message-ttl' => 60000]
        ];

        $consumer = new RabbitMqConsumer($connection, 'test_queue', $queueConfig);

        $this->assertInstanceOf(RabbitMqConsumer::class, $consumer);
    }

    public function testStop(): void
    {
        $connection = $this->createMock(RabbitMqConnection::class);
        $consumer = new RabbitMqConsumer($connection, 'test_queue');

        $consumer->stop();

        $this->assertTrue(true); // No exception
    }

    public function testHandleMessageSuccess(): void
    {
        $connection = $this->createMock(RabbitMqConnection::class);
        $consumer = new RabbitMqConsumer($connection, 'test_queue');

        $mockEnvelope = $this->createMock(AMQPEnvelope::class);
        $mockQueue = $this->createMock(AMQPQueue::class);

        $mockEnvelope->method('getBody')->willReturn('test message');
        $mockEnvelope->method('getDeliveryTag')->willReturn(123);
        $mockEnvelope->method('getRoutingKey')->willReturn('test.key');
        $mockEnvelope->method('getExchangeName')->willReturn('test.exchange');
        $mockEnvelope->method('getHeaders')->willReturn(['header1' => 'value1']);
        $mockEnvelope->method('getTimestamp')->willReturn(1234567890);
        $mockEnvelope->method('getContentType')->willReturn('text/plain');
        $mockEnvelope->method('getContentEncoding')->willReturn('utf-8');
        $mockEnvelope->method('getDeliveryMode')->willReturn(2);
        $mockEnvelope->method('getPriority')->willReturn(5);
        $mockEnvelope->method('getCorrelationId')->willReturn('corr-123');
        $mockEnvelope->method('getReplyTo')->willReturn('reply-queue');
        $mockEnvelope->method('getExpiration')->willReturn('60000');
        $mockEnvelope->method('getMessageId')->willReturn('msg-123');
        $mockEnvelope->method('getAppId')->willReturn('app-123');
        $mockEnvelope->method('getUserId')->willReturn('user-123');

        $mockQueue->expects($this->once())
            ->method('ack')
            ->with(123);

        $callbackInvoked = false;
        $receivedBody = null;
        $receivedMetadata = null;

        $callback = function ($body, $metadata) use (&$callbackInvoked, &$receivedBody, &$receivedMetadata) {
            $callbackInvoked = true;
            $receivedBody = $body;
            $receivedMetadata = $metadata;
            return true; // Continue consuming
        };

        // Use reflection to access private handleMessage method
        $reflection = new \ReflectionClass($consumer);
        $method = $reflection->getMethod('handleMessage');
        $method->setAccessible(true);

        $method->invoke($consumer, $mockEnvelope, $mockQueue, $callback);

        $this->assertTrue($callbackInvoked);
        $this->assertEquals('test message', $receivedBody);
        $this->assertArrayHasKey('routing_key', $receivedMetadata);
        $this->assertEquals('test.key', $receivedMetadata['routing_key']);
        $this->assertEquals(123, $receivedMetadata['delivery_tag']);
        $this->assertEquals('test.exchange', $receivedMetadata['exchange']);
        $this->assertEquals('rabbitmq', $receivedMetadata['type']);
    }

    public function testHandleMessageWithCallbackReturnFalse(): void
    {
        $connection = $this->createMock(RabbitMqConnection::class);
        $consumer = new RabbitMqConsumer($connection, 'test_queue');

        $mockEnvelope = $this->createMock(AMQPEnvelope::class);
        $mockQueue = $this->createMock(AMQPQueue::class);

        $mockEnvelope->method('getBody')->willReturn('stop message');
        $mockEnvelope->method('getDeliveryTag')->willReturn(456);
        $mockEnvelope->method('getRoutingKey')->willReturn('stop.key');
        $mockEnvelope->method('getExchangeName')->willReturn('exchange');
        $mockEnvelope->method('getHeaders')->willReturn([]);
        $mockEnvelope->method('getTimestamp')->willReturn(0);
        $mockEnvelope->method('getContentType')->willReturn('');
        $mockEnvelope->method('getContentEncoding')->willReturn('');
        $mockEnvelope->method('getDeliveryMode')->willReturn(0);
        $mockEnvelope->method('getPriority')->willReturn(0);
        $mockEnvelope->method('getCorrelationId')->willReturn('');
        $mockEnvelope->method('getReplyTo')->willReturn('');
        $mockEnvelope->method('getExpiration')->willReturn('');
        $mockEnvelope->method('getMessageId')->willReturn('');
        $mockEnvelope->method('getAppId')->willReturn('');
        $mockEnvelope->method('getUserId')->willReturn('');

        $mockQueue->expects($this->never())
            ->method('ack');

        $mockQueue->expects($this->once())
            ->method('nack')
            ->with(456, AMQP_REQUEUE);

        $callback = function ($body, $metadata) {
            return false; // Stop consuming — message should be NACKed and requeued
        };

        $reflection = new \ReflectionClass($consumer);
        $method = $reflection->getMethod('handleMessage');
        $method->setAccessible(true);

        $method->invoke($consumer, $mockEnvelope, $mockQueue, $callback);
    }

    public function testHandleMessageWithCallbackException(): void
    {
        $connection = $this->createMock(RabbitMqConnection::class);
        $consumer = new RabbitMqConsumer($connection, 'test_queue');

        $mockEnvelope = $this->createMock(AMQPEnvelope::class);
        $mockQueue = $this->createMock(AMQPQueue::class);

        $mockEnvelope->method('getBody')->willReturn('error message');
        $mockEnvelope->method('getDeliveryTag')->willReturn(789);
        $mockEnvelope->method('getRoutingKey')->willReturn('error.key');
        $mockEnvelope->method('getExchangeName')->willReturn('exchange');
        $mockEnvelope->method('getHeaders')->willReturn([]);
        $mockEnvelope->method('getTimestamp')->willReturn(0);
        $mockEnvelope->method('getContentType')->willReturn('');
        $mockEnvelope->method('getContentEncoding')->willReturn('');
        $mockEnvelope->method('getDeliveryMode')->willReturn(0);
        $mockEnvelope->method('getPriority')->willReturn(0);
        $mockEnvelope->method('getCorrelationId')->willReturn('');
        $mockEnvelope->method('getReplyTo')->willReturn('');
        $mockEnvelope->method('getExpiration')->willReturn('');
        $mockEnvelope->method('getMessageId')->willReturn('');
        $mockEnvelope->method('getAppId')->willReturn('');
        $mockEnvelope->method('getUserId')->willReturn('');

        $mockQueue->expects($this->once())
            ->method('nack')
            ->with(789, AMQP_REQUEUE);

        $callback = function ($body, $metadata) {
            throw new \Exception('Callback error');
        };

        $reflection = new \ReflectionClass($consumer);
        $method = $reflection->getMethod('handleMessage');
        $method->setAccessible(true);

        // Should NACK the message then re-throw
        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Callback error');
        $method->invoke($consumer, $mockEnvelope, $mockQueue, $callback);
    }

    public function testHandleMessageWithNullDeliveryTag(): void
    {
        $connection = $this->createMock(RabbitMqConnection::class);
        $consumer = new RabbitMqConsumer($connection, 'test_queue');

        $mockEnvelope = $this->createMock(AMQPEnvelope::class);
        $mockQueue = $this->createMock(AMQPQueue::class);

        $mockEnvelope->method('getDeliveryTag')->willReturn(null);

        $mockQueue->expects($this->never())->method('ack');
        $mockQueue->expects($this->never())->method('nack');

        $callback = function ($body, $metadata) {
            $this->fail('Callback should not be invoked');
        };

        $reflection = new \ReflectionClass($consumer);
        $method = $reflection->getMethod('handleMessage');
        $method->setAccessible(true);

        $this->expectException(ConsumerException::class);
        $this->expectExceptionMessage('Received message without delivery tag');
        $method->invoke($consumer, $mockEnvelope, $mockQueue, $callback);
    }

    public function testHandleMessageWithNackException(): void
    {
        $connection = $this->createMock(RabbitMqConnection::class);
        $consumer = new RabbitMqConsumer($connection, 'test_queue');

        $mockEnvelope = $this->createMock(AMQPEnvelope::class);
        $mockQueue = $this->createMock(AMQPQueue::class);

        $mockEnvelope->method('getBody')->willReturn('message');
        $mockEnvelope->method('getDeliveryTag')->willReturn(999);
        $mockEnvelope->method('getRoutingKey')->willReturn('key');
        $mockEnvelope->method('getExchangeName')->willReturn('exchange');
        $mockEnvelope->method('getHeaders')->willReturn([]);
        $mockEnvelope->method('getTimestamp')->willReturn(0);
        $mockEnvelope->method('getContentType')->willReturn('');
        $mockEnvelope->method('getContentEncoding')->willReturn('');
        $mockEnvelope->method('getDeliveryMode')->willReturn(0);
        $mockEnvelope->method('getPriority')->willReturn(0);
        $mockEnvelope->method('getCorrelationId')->willReturn('');
        $mockEnvelope->method('getReplyTo')->willReturn('');
        $mockEnvelope->method('getExpiration')->willReturn('');
        $mockEnvelope->method('getMessageId')->willReturn('');
        $mockEnvelope->method('getAppId')->willReturn('');
        $mockEnvelope->method('getUserId')->willReturn('');

        $mockQueue->expects($this->once())
            ->method('nack')
            ->willThrowException(new \AMQPException('Nack failed'));

        $callback = function () {
            throw new \Exception('Callback error');
        };

        $reflection = new \ReflectionClass($consumer);
        $method = $reflection->getMethod('handleMessage');
        $method->setAccessible(true);

        // Original callback exception is re-thrown even if NACK fails
        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Callback error');
        $method->invoke($consumer, $mockEnvelope, $mockQueue, $callback);
    }
}
