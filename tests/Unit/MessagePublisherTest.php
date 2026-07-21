<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit;

use JardisSupport\Contract\Messaging\PublisherInterface;
use JardisSupport\Contract\Messaging\Exception\PublishException;
use JardisAdapter\Messaging\MessagePublisher;
use JardisAdapter\Messaging\Validation\MessageValidator;
use PHPUnit\Framework\TestCase;

class MessagePublisherTest extends TestCase
{
    public function testConstructorWithoutArguments(): void
    {
        $publisher = new MessagePublisher();

        $this->assertInstanceOf(MessagePublisher::class, $publisher);
    }

    public function testThrowsExceptionWhenNoPublishersConfigured(): void
    {
        $publisher = new MessagePublisher();

        $this->expectException(PublishException::class);
        $this->expectExceptionMessage('No publishers configured');

        $publisher->publish('test-topic', 'message');
    }

    public function testPublishWithSinglePublisherSuccess(): void
    {
        $mockPublisher = $this->createMock(PublisherInterface::class);

        $mockPublisher->expects($this->once())
            ->method('publish')
            ->with('test-topic', 'message', [])
            ->willReturn(true);

        $publisher = new MessagePublisher($mockPublisher);

        $result = $publisher->publish('test-topic', 'message');

        $this->assertTrue($result);
    }

    public function testPublishWithSinglePublisherFailure(): void
    {
        $mockPublisher = $this->createMock(PublisherInterface::class);

        $mockPublisher->expects($this->once())
            ->method('publish')
            ->willThrowException(new PublishException('Connection failed'));

        $publisher = new MessagePublisher($mockPublisher);

        $this->expectException(PublishException::class);
        $this->expectExceptionMessage('All publisher layers failed');

        $publisher->publish('test-topic', 'message');
    }

    public function testPublishStringMessage(): void
    {
        $mockPublisher = $this->createMock(PublisherInterface::class);

        $mockPublisher->expects($this->once())
            ->method('publish')
            ->with('test-topic', 'Hello World', [])
            ->willReturn(true);

        $publisher = new MessagePublisher($mockPublisher);

        $result = $publisher->publish('test-topic', 'Hello World');

        $this->assertTrue($result);
    }

    public function testPublishArrayMessage(): void
    {
        $mockPublisher = $this->createMock(PublisherInterface::class);

        $mockPublisher->expects($this->once())
            ->method('publish')
            ->with('test-topic', '{"key":"value"}', [])
            ->willReturn(true);

        $publisher = new MessagePublisher($mockPublisher);

        $result = $publisher->publish('test-topic', ['key' => 'value']);

        $this->assertTrue($result);
    }

    public function testPublishObjectMessage(): void
    {
        $mockPublisher = $this->createMock(PublisherInterface::class);

        $object = new class implements \JsonSerializable {
            public function jsonSerialize(): array
            {
                return ['type' => 'test', 'value' => 123];
            }
        };

        $mockPublisher->expects($this->once())
            ->method('publish')
            ->with('test-topic', '{"type":"test","value":123}', [])
            ->willReturn(true);

        $publisher = new MessagePublisher($mockPublisher);

        $result = $publisher->publish('test-topic', $object);

        $this->assertTrue($result);
    }

    public function testMultiplePublishersFallback(): void
    {
        $mockPublisher1 = $this->createMock(PublisherInterface::class);
        $mockPublisher2 = $this->createMock(PublisherInterface::class);

        $mockPublisher1->expects($this->once())
            ->method('publish')
            ->willThrowException(new PublishException('Publisher 1 failed'));

        $mockPublisher2->expects($this->once())
            ->method('publish')
            ->willReturn(true);

        $publisher = new MessagePublisher($mockPublisher1, $mockPublisher2);

        $result = $publisher->publish('test-topic', 'message');
        $this->assertTrue($result);
    }

    public function testAllPublishersFail(): void
    {
        $mockPublisher1 = $this->createMock(PublisherInterface::class);
        $mockPublisher2 = $this->createMock(PublisherInterface::class);

        $mockPublisher1->expects($this->once())
            ->method('publish')
            ->willThrowException(new PublishException('Publisher 1 failed'));

        $mockPublisher2->expects($this->once())
            ->method('publish')
            ->willThrowException(new PublishException('Publisher 2 failed'));

        $publisher = new MessagePublisher($mockPublisher1, $mockPublisher2);

        $this->expectException(PublishException::class);
        $this->expectExceptionMessage('All publisher layers failed');

        $publisher->publish('test-topic', 'message');
    }

    public function testPublishWithCustomOptions(): void
    {
        $mockPublisher = $this->createMock(PublisherInterface::class);

        $mockPublisher->expects($this->once())
            ->method('publish')
            ->with('test-topic', 'message', ['custom' => 'option'])
            ->willReturn(true);

        $publisher = new MessagePublisher($mockPublisher);

        $result = $publisher->publish('test-topic', 'message', ['custom' => 'option']);

        $this->assertTrue($result);
    }

    public function testSerializeComplexArray(): void
    {
        $mockPublisher = $this->createMock(PublisherInterface::class);

        $complexArray = [
            'user' => [
                'id' => 123,
                'name' => 'John',
                'meta' => [
                    'roles' => ['admin', 'user']
                ]
            ],
            'timestamp' => 1234567890,
            'nullable' => null
        ];

        $mockPublisher->expects($this->once())
            ->method('publish')
            ->with(
                'test-topic',
                $this->isType('string'),
                []
            )
            ->willReturnCallback(function ($topic, $message, $options) use ($complexArray) {
                $decoded = json_decode($message, true);
                $this->assertEquals($complexArray, $decoded);
                return true;
            });

        $publisher = new MessagePublisher($mockPublisher);

        $result = $publisher->publish('test-topic', $complexArray);

        $this->assertTrue($result);
    }

    public function testWithValidatorReturnsNewInstance(): void
    {
        $publisher  = new MessagePublisher();
        $validator  = new MessageValidator();
        $clone      = $publisher->withValidator($validator);

        $this->assertNotSame($publisher, $clone);
        $this->assertInstanceOf(MessagePublisher::class, $clone);
    }

    public function testWithValidatorIsUsedForArraySerialization(): void
    {
        $mockPublisher = $this->createMock(PublisherInterface::class);

        $validator = $this->createMock(MessageValidator::class);
        $validator->expects($this->once())
            ->method('validate')
            ->with(['key' => 'value']);

        $mockPublisher->expects($this->once())
            ->method('publish')
            ->willReturn(true);

        $publisher = (new MessagePublisher($mockPublisher))->withValidator($validator);
        $publisher->publish('test-topic', ['key' => 'value']);
    }

    public function testErrorMessageContainsResolvedLabel(): void
    {
        $mockPublisher = $this->createMock(PublisherInterface::class);

        $mockPublisher->expects($this->once())
            ->method('publish')
            ->willThrowException(new PublishException('Connection refused'));

        $publisher = new MessagePublisher($mockPublisher);

        try {
            $publisher->publish('test-topic', 'message');
            $this->fail('Expected PublishException was not thrown');
        } catch (PublishException $e) {
            $this->assertStringContainsString('Connection refused', $e->getMessage());
        }
    }
}
