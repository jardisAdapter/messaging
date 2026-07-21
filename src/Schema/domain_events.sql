-- Database transport schema for jardisadapter/messaging
-- Create this table in your application database to use the database transport.

CREATE TABLE IF NOT EXISTS domain_events (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    payload TEXT NOT NULL,
    created_at DATETIME(6) NOT NULL,
    processed_at DATETIME(6) NULL DEFAULT NULL,
    attempts TINYINT UNSIGNED NOT NULL DEFAULT 0,
    last_error TEXT NULL DEFAULT NULL,
    INDEX idx_unprocessed (processed_at, created_at),
    INDEX idx_topic (topic, processed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Fan-Out support: each consumer group tracks its own processing status.
-- Required only when using consumer groups (Fan-Out mode).
-- Without this table, the consumer operates in Point-to-Point mode
-- using processed_at directly on domain_events.

CREATE TABLE IF NOT EXISTS domain_event_subscriptions (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    event_id BIGINT UNSIGNED NOT NULL,
    consumer_group VARCHAR(255) NOT NULL,
    processed_at DATETIME(6) NULL DEFAULT NULL,
    attempts TINYINT UNSIGNED NOT NULL DEFAULT 0,
    last_error TEXT NULL DEFAULT NULL,
    UNIQUE INDEX idx_event_group (event_id, consumer_group),
    INDEX idx_pending (consumer_group, processed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
