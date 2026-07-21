<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit\Handler;

use JardisSupport\Contract\Messaging\MessageHandlerInterface;
use JardisAdapter\Messaging\Handler\CallbackHandler;
use PHPUnit\Framework\TestCase;

class CallbackHandlerTest extends TestCase
{
    public function testImplementsMessageHandlerInterface(): void
    {
        $callback = fn() => true;
        $handler = new CallbackHandler($callback);

        $this->assertInstanceOf(MessageHandlerInterface::class, $handler);
    }

    public function testHandlesStringMessage(): void
    {
        $called = false;
        $receivedMessage = null;
        $receivedMeta = null;

        $callback = function (string|array $msg, array $meta) use (&$called, &$receivedMessage, &$receivedMeta): bool {
            $called = true;
            $receivedMessage = $msg;
            $receivedMeta = $meta;
            return true;
        };

        $handler = new CallbackHandler($callback);
        $result = $handler->handle('test message', ['key' => 'value']);

        $this->assertTrue($called);
        $this->assertSame('test message', $receivedMessage);
        $this->assertSame(['key' => 'value'], $receivedMeta);
        $this->assertTrue($result);
    }

    public function testHandlesArrayMessage(): void
    {
        $receivedMessage = null;

        $callback = function (string|array $msg, array $meta) use (&$receivedMessage): bool {
            $receivedMessage = $msg;
            return true;
        };

        $handler = new CallbackHandler($callback);
        $message = ['user' => 'John', 'action' => 'login'];
        $result = $handler->handle($message, []);

        $this->assertSame($message, $receivedMessage);
        $this->assertTrue($result);
    }

    public function testReturnsFalse(): void
    {
        $callback = fn() => false;
        $handler = new CallbackHandler($callback);

        $result = $handler->handle('test', []);

        $this->assertFalse($result);
    }

    public function testPassesMetadata(): void
    {
        $receivedMeta = null;

        $callback = function (string|array $msg, array $meta) use (&$receivedMeta): bool {
            $receivedMeta = $meta;
            return true;
        };

        $handler = new CallbackHandler($callback);
        $metadata = [
            'timestamp' => time(),
            'routing_key' => 'test.key',
            'headers' => ['x-custom' => 'value']
        ];

        $handler->handle('message', $metadata);

        $this->assertSame($metadata, $receivedMeta);
    }

    public function testCanThrowException(): void
    {
        $callback = function (): bool {
            throw new \RuntimeException('Processing failed');
        };

        $handler = new CallbackHandler($callback);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Processing failed');

        $handler->handle('test', []);
    }

    public function testWithComplexLogic(): void
    {
        $processed = [];

        $callback = function (string|array $msg, array $meta) use (&$processed): bool {
            if (is_array($msg) && isset($msg['priority']) && $msg['priority'] === 'high') {
                $processed[] = $msg;
                return true;
            }
            return false;
        };

        $handler = new CallbackHandler($callback);

        // High priority - should process
        $result1 = $handler->handle(['priority' => 'high', 'data' => 'important'], []);
        $this->assertTrue($result1);
        $this->assertCount(1, $processed);

        // Low priority - should reject
        $result2 = $handler->handle(['priority' => 'low', 'data' => 'normal'], []);
        $this->assertFalse($result2);
        $this->assertCount(1, $processed);
    }
}
