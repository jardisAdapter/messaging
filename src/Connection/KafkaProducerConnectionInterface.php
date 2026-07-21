<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Connection;

use JardisSupport\Contract\Connection\ConnectionInterface;
use RdKafka\Producer;

interface KafkaProducerConnectionInterface extends ConnectionInterface
{
    public function getClient(): Producer;
}
