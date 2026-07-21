<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Unit\Config;

use JardisAdapter\Messaging\Config\ConnectionConfig;
use PHPUnit\Framework\TestCase;

class ConnectionConfigTest extends TestCase
{
    public function testCreatesValidConfig(): void
    {
        $config = new ConnectionConfig('localhost', 6379);

        $this->assertSame('localhost', $config->host);
        $this->assertSame(6379, $config->port);
        $this->assertNull($config->username);
        $this->assertNull($config->password);
        $this->assertSame([], $config->options);
    }

    public function testCreatesConfigWithCredentials(): void
    {
        $config = new ConnectionConfig(
            'localhost',
            6379,
            'user',
            'pass',
            ['timeout' => 5]
        );

        $this->assertSame('localhost', $config->host);
        $this->assertSame(6379, $config->port);
        $this->assertSame('user', $config->username);
        $this->assertSame('pass', $config->password);
        $this->assertSame(['timeout' => 5], $config->options);
    }

    public function testThrowsExceptionForEmptyHost(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Host cannot be empty');

        new ConnectionConfig('', 6379);
    }

    public function testThrowsExceptionForInvalidPortZero(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Port must be between 1 and 65535, got 0');

        new ConnectionConfig('localhost', 0);
    }

    public function testThrowsExceptionForInvalidPortNegative(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Port must be between 1 and 65535, got -1');

        new ConnectionConfig('localhost', -1);
    }

    public function testThrowsExceptionForInvalidPortTooHigh(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Port must be between 1 and 65535, got 65536');

        new ConnectionConfig('localhost', 65536);
    }

    public function testFromEnvWithDefaults(): void
    {
        $_ENV['TEST_HOST'] = 'redis.example.com';
        $_ENV['TEST_PORT'] = '6380';

        $config = ConnectionConfig::fromEnv('TEST');

        $this->assertSame('redis.example.com', $config->host);
        $this->assertSame(6380, $config->port);
        $this->assertNull($config->username);
        $this->assertNull($config->password);

        unset($_ENV['TEST_HOST'], $_ENV['TEST_PORT']);
    }

    public function testFromEnvWithCredentials(): void
    {
        $_ENV['TEST_HOST'] = 'redis.example.com';
        $_ENV['TEST_PORT'] = '6380';
        $_ENV['TEST_USERNAME'] = 'admin';
        $_ENV['TEST_PASSWORD'] = 'secret';

        $config = ConnectionConfig::fromEnv('TEST');

        $this->assertSame('redis.example.com', $config->host);
        $this->assertSame(6380, $config->port);
        $this->assertSame('admin', $config->username);
        $this->assertSame('secret', $config->password);

        unset($_ENV['TEST_HOST'], $_ENV['TEST_PORT'], $_ENV['TEST_USERNAME'], $_ENV['TEST_PASSWORD']);
    }

    public function testFromEnvWithUserAlias(): void
    {
        $_ENV['TEST_HOST'] = 'redis.example.com';
        $_ENV['TEST_PORT'] = '6380';
        $_ENV['TEST_USER'] = 'admin';

        $config = ConnectionConfig::fromEnv('TEST');

        $this->assertSame('admin', $config->username);

        unset($_ENV['TEST_HOST'], $_ENV['TEST_PORT'], $_ENV['TEST_USER']);
    }

    public function testFromEnvWithOptions(): void
    {
        $_ENV['TEST_HOST'] = 'redis.example.com';
        $_ENV['TEST_PORT'] = '6380';

        $config = ConnectionConfig::fromEnv('TEST', ['timeout' => 10]);

        $this->assertSame(['timeout' => 10], $config->options);

        unset($_ENV['TEST_HOST'], $_ENV['TEST_PORT']);
    }

    public function testFromEnvFallsBackToDefaults(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Port must be between 1 and 65535, got 0');

        ConnectionConfig::fromEnv('NONEXISTENT');
    }

    public function testFromArray(): void
    {
        $array = [
            'host' => 'kafka.example.com',
            'port' => 9092,
            'username' => 'user',
            'password' => 'pass',
            'options' => ['timeout' => 30]
        ];

        $config = ConnectionConfig::fromArray($array);

        $this->assertSame('kafka.example.com', $config->host);
        $this->assertSame(9092, $config->port);
        $this->assertSame('user', $config->username);
        $this->assertSame('pass', $config->password);
        $this->assertSame(['timeout' => 30], $config->options);
    }

    public function testFromArrayWithDefaults(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Port must be between 1 and 65535, got 0');

        ConnectionConfig::fromArray([]);
    }

    public function testFromArrayWithUserAlias(): void
    {
        $array = [
            'host' => 'redis.example.com',
            'port' => 6379,
            'user' => 'admin'
        ];

        $config = ConnectionConfig::fromArray($array);

        $this->assertSame('admin', $config->username);
    }

    public function testIsReadonly(): void
    {
        $config = new ConnectionConfig('localhost', 6379);

        $this->expectException(\Error::class);

        $config->host = 'other.host';
    }
}
