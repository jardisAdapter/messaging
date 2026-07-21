<?php

declare(strict_types=1);

namespace JardisAdapter\Messaging\Connection;

use JardisSupport\Contract\Connection\ConnectionInterface;
use PDO;

interface DatabaseConnectionInterface extends ConnectionInterface
{
    public function getClient(): PDO;
}
