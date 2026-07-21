<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit;

use JardisSupport\Contract\Messaging\ConsumerInterface;
use JardisSupport\Contract\Messaging\MessageHandlerInterface;
use JardisSupport\Contract\Messaging\Exception\ConsumerException;
use JardisAdapter\Messaging\MessageConsumer;
use PHPUnit\Framework\TestCase;

class MessageConsumerTest extends TestCase
{
    public function testConstructorWithoutArguments(): void
    {
        $consumer = new MessageConsumer();

        $this->assertInstanceOf(MessageConsumer::class, $consumer);
    }

    public function testThrowsExceptionWhenNoConsumersConfigured(): void
    {
        $consumer = new MessageConsumer();
        $handler = $this->createMock(MessageHandlerInterface::class);

        $this->expectException(ConsumerException::class);
        $this->expectExceptionMessage('No consumers configured');

        $consumer->consume('test-topic', $handler);
    }

    public function testStopWithNoConsumers(): void
    {
        $consumer = new MessageConsumer();
        $consumer->stop();

        $this->assertTrue(true);
    }

    public function testStopWithConsumer(): void
    {
        $mockConsumer = $this->createMock(ConsumerInterface::class);
        $mockConsumer->expects($this->once())
            ->method('stop');

        $consumer = new MessageConsumer($mockConsumer);

        $consumer->stop();
    }

    public function testConsumeWithSingleConsumerSuccess(): void
    {
        $mockConsumer = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        $mockConsumer->expects($this->once())
            ->method('consume')
            ->with('test-topic', $this->isType('callable'), []);

        $consumer = new MessageConsumer($mockConsumer);

        $consumer->consume('test-topic', $mockHandler);
    }

    public function testConsumeWithSingleConsumerFailure(): void
    {
        $mockConsumer = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        $mockConsumer->expects($this->once())
            ->method('consume')
            ->willThrowException(new ConsumerException('Connection failed'));

        $consumer = new MessageConsumer($mockConsumer);

        $this->expectException(ConsumerException::class);
        $this->expectExceptionMessage('All consumer layers failed');

        $consumer->consume('test-topic', $mockHandler);
    }

    public function testConsumeCallbackDeserializesJson(): void
    {
        $mockConsumer = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        $capturedCallback = null;

        $mockConsumer->expects($this->once())
            ->method('consume')
            ->willReturnCallback(function ($topic, $callback, $options) use (&$capturedCallback) {
                $capturedCallback = $callback;
            });

        $consumer = new MessageConsumer($mockConsumer);

        $consumer->consume('test-topic', $mockHandler);

        $mockHandler->expects($this->once())
            ->method('handle')
            ->with(['key' => 'value'], ['meta' => 'data'])
            ->willReturn(true);

        $result = $capturedCallback('{"key":"value"}', ['meta' => 'data']);
        $this->assertTrue($result);
    }

    public function testDeserializeValidJson(): void
    {
        $mockConsumer = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        $capturedCallback = null;

        $mockConsumer->expects($this->once())
            ->method('consume')
            ->willReturnCallback(function ($topic, $callback, $options) use (&$capturedCallback) {
                $capturedCallback = $callback;
            });

        $consumer = new MessageConsumer($mockConsumer);

        $consumer->consume('test-topic', $mockHandler);

        $mockHandler->expects($this->once())
            ->method('handle')
            ->with(['name' => 'John', 'age' => 30], [])
            ->willReturn(true);

        $capturedCallback('{"name":"John","age":30}', []);
    }

    public function testDeserializeInvalidJson(): void
    {
        $mockConsumer = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        $capturedCallback = null;

        $mockConsumer->expects($this->once())
            ->method('consume')
            ->willReturnCallback(function ($topic, $callback, $options) use (&$capturedCallback) {
                $capturedCallback = $callback;
            });

        $consumer = new MessageConsumer($mockConsumer);

        $consumer->consume('test-topic', $mockHandler);

        $mockHandler->expects($this->once())
            ->method('handle')
            ->with('not-json', [])
            ->willReturn(true);

        $capturedCallback('not-json', []);
    }

    public function testDeserializeEmptyString(): void
    {
        $mockConsumer = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        $capturedCallback = null;

        $mockConsumer->expects($this->once())
            ->method('consume')
            ->willReturnCallback(function ($topic, $callback, $options) use (&$capturedCallback) {
                $capturedCallback = $callback;
            });

        $consumer = new MessageConsumer($mockConsumer);

        $consumer->consume('test-topic', $mockHandler);

        $mockHandler->expects($this->once())
            ->method('handle')
            ->with('', [])
            ->willReturn(true);

        $capturedCallback('', []);
    }

    public function testDeserializeJsonNumber(): void
    {
        $mockConsumer = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        $capturedCallback = null;

        $mockConsumer->expects($this->once())
            ->method('consume')
            ->willReturnCallback(function ($topic, $callback, $options) use (&$capturedCallback) {
                $capturedCallback = $callback;
            });

        $consumer = new MessageConsumer($mockConsumer);

        $consumer->consume('test-topic', $mockHandler);

        $mockHandler->expects($this->once())
            ->method('handle')
            ->with('123', [])
            ->willReturn(true);

        $capturedCallback('123', []);
    }

    public function testDeserializeJsonString(): void
    {
        $mockConsumer = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        $capturedCallback = null;

        $mockConsumer->expects($this->once())
            ->method('consume')
            ->willReturnCallback(function ($topic, $callback, $options) use (&$capturedCallback) {
                $capturedCallback = $callback;
            });

        $consumer = new MessageConsumer($mockConsumer);

        $consumer->consume('test-topic', $mockHandler);

        $mockHandler->expects($this->once())
            ->method('handle')
            ->with('"hello"', [])
            ->willReturn(true);

        $capturedCallback('"hello"', []);
    }

    public function testMultipleConsumersFallback(): void
    {
        $mockConsumer1 = $this->createMock(ConsumerInterface::class);
        $mockConsumer2 = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        $mockConsumer1->expects($this->once())
            ->method('consume')
            ->willThrowException(new ConsumerException('Consumer 1 failed'));

        $mockConsumer2->expects($this->once())
            ->method('consume');

        $consumer = new MessageConsumer($mockConsumer1, $mockConsumer2);

        $consumer->consume('test-topic', $mockHandler);
    }

    public function testAllConsumersFail(): void
    {
        $mockConsumer1 = $this->createMock(ConsumerInterface::class);
        $mockConsumer2 = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        $mockConsumer1->expects($this->once())
            ->method('consume')
            ->willThrowException(new ConsumerException('Consumer 1 failed'));

        $mockConsumer2->expects($this->once())
            ->method('consume')
            ->willThrowException(new ConsumerException('Consumer 2 failed'));

        $consumer = new MessageConsumer($mockConsumer1, $mockConsumer2);

        $this->expectException(ConsumerException::class);
        $this->expectExceptionMessage('All consumer layers failed');

        $consumer->consume('test-topic', $mockHandler);
    }

    public function testErrorMessageContainsResolvedLabel(): void
    {
        $mockConsumer = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        $mockConsumer->expects($this->once())
            ->method('consume')
            ->willThrowException(new ConsumerException('Connection refused'));

        $consumer = new MessageConsumer($mockConsumer);

        try {
            $consumer->consume('test-topic', $mockHandler);
            $this->fail('Expected ConsumerException was not thrown');
        } catch (ConsumerException $e) {
            $this->assertStringContainsString('Connection refused', $e->getMessage());
        }
    }
}
