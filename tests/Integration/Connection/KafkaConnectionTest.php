<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Tests\Integration\Connection;

use JardisAdapter\Messaging\Config\ConnectionConfig;
use JardisAdapter\Messaging\Connection\KafkaConnection;
use JardisSupport\Contract\Messaging\Exception\ConnectionException;
use PHPUnit\Framework\TestCase;
use RdKafka\Producer;

class KafkaConnectionTest extends TestCase
{
    private ConnectionConfig $config;

    protected function setUp(): void
    {
        $brokers = $_ENV['KAFKA_BROKERS'] ?? 'kafka:9092';
        [$host, $port] = explode(':', $brokers);

        $this->config = new ConnectionConfig(
            host: $host,
            port: (int)$port
        );
    }

    public function testConnectsSuccessfully(): void
    {
        $connection = new KafkaConnection($this->config);
        $connection->connect();

        $this->assertTrue($connection->isConnected());

        $connection->disconnect();
    }

    public function testIsNotConnectedInitially(): void
    {
        $connection = new KafkaConnection($this->config);

        $this->assertFalse($connection->isConnected());
    }

    public function testDisconnectsSuccessfully(): void
    {
        $connection = new KafkaConnection($this->config);
        $connection->connect();

        $this->assertTrue($connection->isConnected());

        $connection->disconnect();

        $this->assertFalse($connection->isConnected());
    }

    public function testReconnectsAfterDisconnect(): void
    {
        $connection = new KafkaConnection($this->config);

        $connection->connect();
        $connection->disconnect();
        $connection->connect();

        $this->assertTrue($connection->isConnected());

        $connection->disconnect();
    }

    public function testConnectIsIdempotent(): void
    {
        $connection = new KafkaConnection($this->config);

        $connection->connect();
        $connection->connect(); // Should not throw
        $connection->connect(); // Should not throw

        $this->assertTrue($connection->isConnected());

        $connection->disconnect();
    }

    public function testDisconnectIsIdempotent(): void
    {
        $connection = new KafkaConnection($this->config);
        $connection->connect();

        $connection->disconnect();
        $connection->disconnect(); // Should not throw
        $connection->disconnect(); // Should not throw

        $this->assertFalse($connection->isConnected());
    }

    public function testGetClientReturnsProducerInstance(): void
    {
        $connection = new KafkaConnection($this->config);
        $connection->connect();

        $client = $connection->getClient();

        $this->assertInstanceOf(Producer::class, $client);

        $connection->disconnect();
    }

    public function testGetClientThrowsWhenNotConnected(): void
    {
        $connection = new KafkaConnection($this->config);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Not connected to Kafka');

        $connection->getClient();
    }

    public function testClientCanCreateTopics(): void
    {
        $connection = new KafkaConnection($this->config);
        $connection->connect();

        $producer = $connection->getClient();
        $topic = $producer->newTopic('test-topic');

        $this->assertInstanceOf(\RdKafka\ProducerTopic::class, $topic);

        $connection->disconnect();
    }

    public function testDisconnectsOnDestruct(): void
    {
        $connection = new KafkaConnection($this->config);
        $connection->connect();

        $this->assertTrue($connection->isConnected());

        unset($connection); // Trigger __destruct

        // Create new connection to verify previous was cleaned up
        $newConnection = new KafkaConnection($this->config);
        $newConnection->connect();

        $this->assertTrue($newConnection->isConnected());

        $newConnection->disconnect();
    }

    public function testConfiguresWithOptions(): void
    {
        $config = new ConnectionConfig(
            host: $this->config->host,
            port: $this->config->port,
            options: [
                'client.id' => 'test-client',
                'socket.timeout.ms' => '10000'
            ]
        );

        $connection = new KafkaConnection($config);
        $connection->connect();

        $this->assertTrue($connection->isConnected());

        $connection->disconnect();
    }
}
