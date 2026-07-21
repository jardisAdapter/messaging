<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit\Consumer;

use JardisAdapter\Messaging\Connection\KafkaConsumerConnectionInterface;
use JardisAdapter\Messaging\Consumer\KafkaConsumer;
use JardisSupport\Contract\Messaging\Exception\ConsumerException;
use PHPUnit\Framework\TestCase;
use RdKafka\KafkaConsumer as RdKafkaConsumer;
use RdKafka\Message;

class KafkaConsumerTest extends TestCase
{
    public function testConstructor(): void
    {
        $connection = $this->createMock(KafkaConsumerConnectionInterface::class);
        $consumer = new KafkaConsumer($connection);

        $this->assertInstanceOf(KafkaConsumer::class, $consumer);
    }

    public function testStop(): void
    {
        $connection = $this->createMock(KafkaConsumerConnectionInterface::class);
        $consumer = new KafkaConsumer($connection);

        $consumer->stop();

        $this->assertTrue(true); // No exception
    }

    public function testGetConnection(): void
    {
        $connection = $this->createMock(KafkaConsumerConnectionInterface::class);
        $consumer = new KafkaConsumer($connection);

        $this->assertSame($connection, $consumer->getConnection());
    }

    public function testHandleMessageWithNoError(): void
    {
        $connection = $this->createMock(KafkaConsumerConnectionInterface::class);
        $consumer = new KafkaConsumer($connection);

        $mockRdConsumer = $this->createMock(RdKafkaConsumer::class);

        $mockMessage = $this->createMock(Message::class);
        $mockMessage->err = RD_KAFKA_RESP_ERR_NO_ERROR;
        $mockMessage->payload = 'test payload';
        $mockMessage->partition = 0;
        $mockMessage->offset = 123;
        $mockMessage->timestamp = 1234567890;
        $mockMessage->key = 'test-key';
        $mockMessage->topic_name = 'test-topic';

        $callbackInvoked = false;
        $receivedPayload = null;
        $receivedMetadata = null;

        $callback = function ($payload, $metadata) use (&$callbackInvoked, &$receivedPayload, &$receivedMetadata) {
            $callbackInvoked = true;
            $receivedPayload = $payload;
            $receivedMetadata = $metadata;
            return true;
        };

        $mockRdConsumer->expects($this->once())->method('commit');

        $reflection = new \ReflectionClass($consumer);
        $method = $reflection->getMethod('handleMessage');
        $method->setAccessible(true);

        $result = $method->invoke($consumer, $mockMessage, $mockRdConsumer, $callback);

        $this->assertTrue($result);
        $this->assertTrue($callbackInvoked);
        $this->assertEquals('test payload', $receivedPayload);
        $this->assertArrayHasKey('partition', $receivedMetadata);
        $this->assertEquals(0, $receivedMetadata['partition']);
        $this->assertEquals(123, $receivedMetadata['offset']);
        $this->assertEquals('test-topic', $receivedMetadata['topic']);
        $this->assertEquals('kafka', $receivedMetadata['type']);
    }

    public function testHandleMessagePartitionEof(): void
    {
        $connection = $this->createMock(KafkaConsumerConnectionInterface::class);
        $consumer = new KafkaConsumer($connection);
        $mockRdConsumer = $this->createMock(RdKafkaConsumer::class);

        $mockMessage = $this->createMock(Message::class);
        $mockMessage->err = RD_KAFKA_RESP_ERR__PARTITION_EOF;

        $callback = function () {
            $this->fail('Callback should not be invoked for EOF');
        };

        $reflection = new \ReflectionClass($consumer);
        $method = $reflection->getMethod('handleMessage');
        $method->setAccessible(true);

        $result = $method->invoke($consumer, $mockMessage, $mockRdConsumer, $callback);

        $this->assertFalse($result);
    }

    public function testHandleMessageTimeout(): void
    {
        $connection = $this->createMock(KafkaConsumerConnectionInterface::class);
        $consumer = new KafkaConsumer($connection);
        $mockRdConsumer = $this->createMock(RdKafkaConsumer::class);

        $mockMessage = $this->createMock(Message::class);
        $mockMessage->err = RD_KAFKA_RESP_ERR__TIMED_OUT;

        $callback = function () {
            $this->fail('Callback should not be invoked for timeout');
        };

        $reflection = new \ReflectionClass($consumer);
        $method = $reflection->getMethod('handleMessage');
        $method->setAccessible(true);

        $result = $method->invoke($consumer, $mockMessage, $mockRdConsumer, $callback);

        $this->assertFalse($result);
    }

    public function testHandleMessageWithCallbackReturnFalse(): void
    {
        $connection = $this->createMock(KafkaConsumerConnectionInterface::class);
        $consumer = new KafkaConsumer($connection);
        $mockRdConsumer = $this->createMock(RdKafkaConsumer::class);

        $mockMessage = $this->createMock(Message::class);
        $mockMessage->err = RD_KAFKA_RESP_ERR_NO_ERROR;
        $mockMessage->payload = 'test';
        $mockMessage->partition = 0;
        $mockMessage->offset = 0;
        $mockMessage->timestamp = 0;
        $mockMessage->key = null;
        $mockMessage->topic_name = 'test';

        $callback = function () {
            return false;
        };

        $reflection = new \ReflectionClass($consumer);
        $method = $reflection->getMethod('handleMessage');
        $method->setAccessible(true);

        $result = $method->invoke($consumer, $mockMessage, $mockRdConsumer, $callback);

        $this->assertTrue($result);
    }

    public function testHandleMessageWithCallbackException(): void
    {
        $connection = $this->createMock(KafkaConsumerConnectionInterface::class);
        $consumer = new KafkaConsumer($connection);
        $mockRdConsumer = $this->createMock(RdKafkaConsumer::class);

        $mockMessage = $this->createMock(Message::class);
        $mockMessage->err = RD_KAFKA_RESP_ERR_NO_ERROR;
        $mockMessage->payload = 'test';
        $mockMessage->partition = 0;
        $mockMessage->offset = 0;
        $mockMessage->timestamp = 0;
        $mockMessage->key = null;
        $mockMessage->topic_name = 'test';

        $callback = function () {
            throw new \Exception('Callback error');
        };

        $reflection = new \ReflectionClass($consumer);
        $method = $reflection->getMethod('handleMessage');
        $method->setAccessible(true);

        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Callback error');
        $method->invoke($consumer, $mockMessage, $mockRdConsumer, $callback);
    }

    public function testHandleMessageWithUnknownError(): void
    {
        $connection = $this->createMock(KafkaConsumerConnectionInterface::class);
        $consumer = new KafkaConsumer($connection);
        $mockRdConsumer = $this->createMock(RdKafkaConsumer::class);

        $mockMessage = $this->createMock(Message::class);
        $mockMessage->err = 999;
        $mockMessage->method('errstr')->willReturn('Unknown error');

        $callback = function () {
            $this->fail('Callback should not be invoked for errors');
        };

        $reflection = new \ReflectionClass($consumer);
        $runningProperty = $reflection->getProperty('running');
        $runningProperty->setAccessible(true);
        $runningProperty->setValue($consumer, true);

        $method = $reflection->getMethod('handleMessage');
        $method->setAccessible(true);

        $this->expectException(ConsumerException::class);
        $this->expectExceptionMessage('Kafka consumer error: Unknown error (code: 999)');
        $method->invoke($consumer, $mockMessage, $mockRdConsumer, $callback);
    }
}
