<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit\Exception;

use JardisSupport\Contract\Messaging\Exception\ConsumerException;
use JardisSupport\Contract\Messaging\Exception\MessageException;
use PHPUnit\Framework\TestCase;

class ConsumerExceptionTest extends TestCase
{
    public function testExtendsMessageException(): void
    {
        $exception = new ConsumerException('consumer failed');

        $this->assertInstanceOf(MessageException::class, $exception);
        $this->assertInstanceOf(\Exception::class, $exception);
        $this->assertSame('consumer failed', $exception->getMessage());
    }

    public function testWithCode(): void
    {
        $exception = new ConsumerException('error', 500);

        $this->assertSame('error', $exception->getMessage());
        $this->assertSame(500, $exception->getCode());
    }

    public function testWithPrevious(): void
    {
        $previous = new \RuntimeException('original error');
        $exception = new ConsumerException('wrapped error', previous: $previous);

        $this->assertSame('wrapped error', $exception->getMessage());
        $this->assertSame($previous, $exception->getPrevious());
    }
}
