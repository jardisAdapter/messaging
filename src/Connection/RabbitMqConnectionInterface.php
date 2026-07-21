<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Connection;

use AMQPChannel;
use AMQPConnection;
use AMQPExchange;
use JardisSupport\Contract\Connection\ConnectionInterface;

interface RabbitMqConnectionInterface extends ConnectionInterface
{
    public function getExchange(): AMQPExchange;

    public function getChannel(): AMQPChannel;

    public function getConnection(): AMQPConnection;
}
