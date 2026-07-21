<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Connection;

use JardisSupport\Contract\Connection\ConnectionInterface;
use RdKafka\KafkaConsumer;

interface KafkaConsumerConnectionInterface extends ConnectionInterface
{
    public function getClient(): KafkaConsumer;
}
