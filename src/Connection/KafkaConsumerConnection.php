<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Connection;

use JardisAdapter\Messaging\Config\ConnectionConfig;
use JardisSupport\Contract\Messaging\Exception\ConnectionException;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use Exception;

/**
 * Kafka consumer connection manager
 *
 * Handles connection lifecycle to Kafka broker for consuming messages.
 */
class KafkaConsumerConnection implements KafkaConsumerConnectionInterface
{
    private ?KafkaConsumer $consumer = null;
    private bool $connected = false;

    public function __construct(
        private readonly ConnectionConfig $config,
        private readonly string $groupId,
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
            $conf = new Conf();
            $brokers = str_contains($this->config->host, ':') || str_contains($this->config->host, ',')
                ? $this->config->host
                : "{$this->config->host}:{$this->config->port}";
            $conf->set('metadata.broker.list', $brokers);
            $conf->set('group.id', $this->groupId);
            $conf->set('auto.offset.reset', 'earliest');

            if ($this->config->username !== null && $this->config->password !== null) {
                $conf->set('security.protocol', 'SASL_SSL');
                $conf->set('sasl.mechanism', 'PLAIN');
                $conf->set('sasl.username', $this->config->username);
                $conf->set('sasl.password', $this->config->password);
            }

            foreach ($this->config->options as $key => $value) {
                if (is_string($value) || is_int($value)) {
                    $conf->set($key, (string) $value);
                }
            }

            $this->consumer = new KafkaConsumer($conf);
            $this->connected = true;
        } catch (Exception $e) {
            throw new ConnectionException(
                "Failed to create Kafka consumer: {$e->getMessage()}",
                previous: $e
            );
        }
    }

    public function disconnect(): void
    {
        if ($this->consumer !== null && $this->connected) {
            $this->consumer = null;
            $this->connected = false;
        }
    }

    public function isConnected(): bool
    {
        return $this->connected && $this->consumer !== null;
    }

    /**
     * @throws ConnectionException if not connected
     */
    public function getClient(): KafkaConsumer
    {
        if (!$this->isConnected() || $this->consumer === null) {
            throw new ConnectionException('Not connected to Kafka');
        }

        return $this->consumer;
    }
}
