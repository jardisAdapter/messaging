<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit\Validation;

use JardisSupport\Contract\Messaging\Exception\PublishException;
use JardisAdapter\Messaging\Validation\MessageValidator;
use PHPUnit\Framework\TestCase;

class MessageValidatorTest extends TestCase
{
    private MessageValidator $validator;

    protected function setUp(): void
    {
        $this->validator = new MessageValidator();
    }

    public function testValidatesSimpleArray(): void
    {
        $data = ['name' => 'John', 'age' => 30, 'active' => true];

        $this->validator->validate($data);

        $this->assertTrue(true); // No exception thrown
    }

    public function testValidatesNestedArray(): void
    {
        $data = [
            'user' => [
                'name' => 'John',
                'meta' => [
                    'role' => 'admin',
                    'permissions' => ['read', 'write']
                ]
            ]
        ];

        $this->validator->validate($data);

        $this->assertTrue(true);
    }

    public function testValidatesArrayWithNull(): void
    {
        $data = ['name' => 'John', 'email' => null];

        $this->validator->validate($data);

        $this->assertTrue(true);
    }

    public function testValidatesArrayWithJsonSerializable(): void
    {
        $object = new class implements \JsonSerializable {
            public function jsonSerialize(): array
            {
                return ['test' => 'value'];
            }
        };

        $data = ['object' => $object];

        $this->validator->validate($data);

        $this->assertTrue(true);
    }

    public function testValidatesArrayWithStringable(): void
    {
        $object = new class implements \Stringable {
            public function __toString(): string
            {
                return 'test';
            }
        };

        $data = ['object' => $object];

        $this->validator->validate($data);

        $this->assertTrue(true);
    }

    public function testValidatesArrayWithDateTime(): void
    {
        $data = ['created_at' => new \DateTime('2024-01-01')];

        $this->validator->validate($data);

        $this->assertTrue(true);
    }

    public function testThrowsExceptionForResource(): void
    {
        $resource = fopen('php://memory', 'r');
        $data = ['file' => $resource];

        $this->expectException(PublishException::class);
        $this->expectExceptionMessage("Cannot serialize resource at key 'file'");

        $this->validator->validate($data);

        fclose($resource);
    }

    public function testThrowsExceptionForClosure(): void
    {
        $data = ['callback' => fn() => 'test'];

        $this->expectException(PublishException::class);
        $this->expectExceptionMessage("Cannot serialize closure at key 'callback'");

        $this->validator->validate($data);
    }

    public function testThrowsExceptionForNonSerializableObject(): void
    {
        $object = new \stdClass();
        $object->name = 'test';

        $data = ['object' => $object];

        $this->expectException(PublishException::class);
        $this->expectExceptionMessageMatches("/Object of type '.*stdClass' at key 'object' is not JSON serializable/");

        $this->validator->validate($data);
    }

    public function testThrowsExceptionForNestedResource(): void
    {
        $resource = fopen('php://memory', 'r');
        $data = [
            'user' => [
                'name' => 'John',
                'file' => $resource
            ]
        ];

        $this->expectException(PublishException::class);
        $this->expectExceptionMessage("Cannot serialize resource at key 'file'");

        $this->validator->validate($data);

        fclose($resource);
    }

    public function testValidatesEmptyArray(): void
    {
        $data = [];

        $this->validator->validate($data);

        $this->assertTrue(true);
    }

    public function testValidatesArrayWithScalarTypes(): void
    {
        $data = [
            'string' => 'test',
            'int' => 42,
            'float' => 3.14,
            'bool' => true,
            'null' => null
        ];

        $this->validator->validate($data);

        $this->assertTrue(true);
    }
}
