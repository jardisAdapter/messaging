<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Connection;

use JardisSupport\Contract\Messaging\Exception\ConnectionException;
use PDO;
use PDOException;

/**
 * PDO-based database connection for the database transport
 *
 * Provides lazy connection establishment and lifecycle management
 * for the DatabasePublisher and DatabaseConsumer.
 */
class DatabaseConnection implements DatabaseConnectionInterface
{
    private ?PDO $pdo = null;
    private bool $connected = false;

    /**
     * @param string $dsn PDO Data Source Name (e.g., 'mysql:host=localhost;dbname=app')
     * @param string|null $username Database username
     * @param string|null $password Database password
     * @param array<int, mixed> $options PDO driver options
     */
    public function __construct(
        private readonly string $dsn,
        private readonly ?string $username = null,
        private readonly ?string $password = null,
        private readonly array $options = [],
    ) {
    }

    public function __destruct()
    {
        $this->disconnect();
    }

    /**
     * @throws ConnectionException
     */
    public function connect(): void
    {
        if ($this->connected) {
            return;
        }

        try {
            $this->pdo = new PDO(
                $this->dsn,
                $this->username,
                $this->password,
                [
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                    PDO::ATTR_EMULATE_PREPARES => false,
                    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                    ...$this->options,
                ]
            );

            $this->connected = true;
        } catch (PDOException $e) {
            throw new ConnectionException(
                "Database connection error: {$e->getMessage()}",
                previous: $e
            );
        }
    }

    public function disconnect(): void
    {
        if ($this->connected) {
            $this->pdo = null;
            $this->connected = false;
        }
    }

    public function isConnected(): bool
    {
        return $this->connected && $this->pdo !== null;
    }

    /**
     * Get the PDO instance
     *
     * @throws ConnectionException if not connected
     */
    public function getClient(): PDO
    {
        if (!$this->isConnected() || $this->pdo === null) {
            throw new ConnectionException('Not connected to database. Call connect() first.');
        }

        return $this->pdo;
    }
}
