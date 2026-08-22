<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Config;

use InvalidArgumentException;

/**
 * Configuration class for message broker connections
 *
 * Provides a flexible way to configure connection parameters
 * for different messaging backends
 */
readonly class ConnectionConfig
{
    /**
     * @param string $host Broker host
     * @param int $port Broker port (1-65535)
     * @param string|null $username Username for authentication
     * @param string|null $password Password for authentication
     * @param array<string, mixed> $options Additional adapter-specific options
     * @throws InvalidArgumentException If host or port are invalid
     */
    public function __construct(
        public string $host,
        public int $port,
        public ?string $username = null,
        public ?string $password = null,
        public array $options = []
    ) {
        if ($host === '') {
            throw new InvalidArgumentException('Host cannot be empty');
        }

        if ($port <= 0 || $port > 65535) {
            throw new InvalidArgumentException(
                "Port must be between 1 and 65535, got {$port}"
            );
        }
    }

    /**
     * Create configuration from environment variables
     *
     * @param string $prefix Environment variable prefix (e.g., 'REDIS', 'KAFKA', 'RABBITMQ')
     * @param array<string, mixed> $options Additional options to merge
     * @return self
     *
     * Kafka note: with prefix 'KAFKA' this reads KAFKA_HOST/KAFKA_PORT, not
     * the canonical KAFKA_BROKERS. ConnectionFactory::kafka()/kafkaConsumer()
     * ignore the port entirely — they take the broker list
     * (host:port[,host:port,...]) straight from the host field. Callers
     * wiring Kafka from env should pass the KAFKA_BROKERS value into the
     * host field directly rather than relying on this generic helper.
     */
    public static function fromEnv(string $prefix, array $options = []): self
    {
        $host = $_ENV["{$prefix}_HOST"] ?? 'localhost';
        $port = (int) ($_ENV["{$prefix}_PORT"] ?? 0);
        $username = $_ENV["{$prefix}_USER"] ?? $_ENV["{$prefix}_USERNAME"] ?? null;
        $password = $_ENV["{$prefix}_PASSWORD"] ?? null;

        return new self($host, $port, $username, $password, $options);
    }

    /**
     * Create configuration from array
     *
     * @param array<string, mixed> $config Configuration array
     * @return self
     */
    public static function fromArray(array $config): self
    {
        return new self(
            host: $config['host'] ?? 'localhost',
            port: (int) ($config['port'] ?? 0),
            username: $config['username'] ?? $config['user'] ?? null,
            password: $config['password'] ?? null,
            options: $config['options'] ?? []
        );
    }
}
