<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Connection;

use JardisSupport\Contract\Connection\ConnectionInterface;
use Redis;

interface RedisConnectionInterface extends ConnectionInterface
{
    public function getClient(): Redis;
}
