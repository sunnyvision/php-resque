<?php

namespace Resque\Logger\Handler;

use Predis\Client;
use Monolog\Logger;
use Monolog\Formatter\JsonFormatter;
use Monolog\Handler\AbstractProcessingHandler;

/**
 * Simple handler that publishes log entries to Redis channels so that they can
 * propagate to other clients subscribed to those channels.
 *
 * By default each log entry is JSON-encoded and then published to a channel
 * following this naming scheme: $LOG_CHANNEL.$LOG_LEVEL.
 *
 * Clients can use SUBSCRIBE or PSUBSCRIBE to control which channels or log
 * levels they are interested in consuming.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class RedisPublishHandler extends AbstractProcessingHandler
{
    private $_redis;

    /**
     * @param mixed $parameters Connection parameters to the Redis server or a client instance
     * @param integer $level The minimum logging level at which this handler will be triggered
     * @param bool $bubble Whether the messages that are handled can bubble up the stack or not
     */
    public function __construct($parameters, $level = Logger::DEBUG, $bubble = true)
    {
        parent::__construct($level, $bubble);
        $this->_redis = $this->createConnection($parameters);
    }

    /**
     * Initialize the client instance used to handle the connection to Redis.
     *
     * It is possible to use a different client library (e.g. phpredis) simply
     * by overriding this method in a subclass.
     *
     * @param mixed $parameters Connection parameters to the Redis server or a client instance
     * @return Predis\Client
     */
    protected function createConnection($parameters)
    {
        return $parameters instanceof Client ? $parameters : new Client($parameters);
    }

    /**
     * {@inheritDoc}
     */
    protected function getDefaultFormatter()
    {
        return new JsonFormatter();
    }

    /**
     * {@inheritDoc}
     */
    protected function write(array $record)
    {
        $channel = $this->formatChannel($record);
        $this->_redis->publish($channel, $record['formatted']);
    }

    /**
     * Generate from the log entry the name of the Redis channel that will be
     * used by the PUBLISH command.
     *
     * @param array $record The record to handle
     * @return string
     */
    protected function formatChannel(array $record)
    {
        return $record['channel'] . '.' . strtolower($record['level_name']);
    }
}