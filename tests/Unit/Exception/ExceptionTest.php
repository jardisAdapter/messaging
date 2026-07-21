<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit\Exception;

use JardisSupport\Contract\Messaging\Exception\ConnectionException;
use JardisSupport\Contract\Messaging\Exception\MessageException;
use JardisSupport\Contract\Messaging\Exception\PublishException;
use PHPUnit\Framework\TestCase;

class ExceptionTest extends TestCase
{
    public function testMessageExceptionExtendsException(): void
    {
        $exception = new MessageException('test message');

        $this->assertInstanceOf(\Exception::class, $exception);
        $this->assertSame('test message', $exception->getMessage());
    }

    public function testPublishExceptionExtendsMessageException(): void
    {
        $exception = new PublishException('publish failed');

        $this->assertInstanceOf(MessageException::class, $exception);
        $this->assertInstanceOf(\Exception::class, $exception);
        $this->assertSame('publish failed', $exception->getMessage());
    }

    public function testConnectionExceptionExtendsMessageException(): void
    {
        $exception = new ConnectionException('connection failed');

        $this->assertInstanceOf(MessageException::class, $exception);
        $this->assertInstanceOf(\Exception::class, $exception);
        $this->assertSame('connection failed', $exception->getMessage());
    }

    public function testExceptionWithCode(): void
    {
        $exception = new PublishException('error', 123);

        $this->assertSame('error', $exception->getMessage());
        $this->assertSame(123, $exception->getCode());
    }

    public function testExceptionWithPrevious(): void
    {
        $previous = new \RuntimeException('original error');
        $exception = new ConnectionException('wrapped error', previous: $previous);

        $this->assertSame('wrapped error', $exception->getMessage());
        $this->assertSame($previous, $exception->getPrevious());
    }

    public function testExceptionChaining(): void
    {
        $root = new \RuntimeException('root cause');
        $middle = new MessageException('middle layer', previous: $root);
        $top = new PublishException('top layer', previous: $middle);

        $this->assertSame($middle, $top->getPrevious());
        $this->assertSame($root, $top->getPrevious()?->getPrevious());
    }
}
