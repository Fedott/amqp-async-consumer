<?php

namespace Fedot\Amqp;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;

abstract class AmqpAbstract
{
    /**
     * @var AMQPStreamConnection
     */
    protected $connection;

    /**
     * @var AMQPChannel
     */
    protected $channel;

    /**
     * @var array
     */
    protected $exchangeOptions = [
        'type' => 'fanout',
        'passive' => false,
        'durable' => true,
        'auto_delete' => false,
        'internal' => false,
        'nowait' => false,
        'arguments' => null,
        'ticket' => null,
    ];

    /**
     * @var array
     */
    protected $queueOptions = [
        'passive' => false,
        'durable' => true,
        'exclusive' => false,
        'auto_delete' => false,
        'nowait' => false,
        'arguments' => null,
        'ticket' => null,
    ];

    protected $qosOptions = [
        'prefetch_size' => null,
        'prefetch_count' => 10000,
        'global' => null,
    ];

    /**
     * @param AMQPStreamConnection $connection
     */
    public function setConnection(AMQPStreamConnection $connection)
    {
        $this->connection = $connection;
    }

    /**
     * @param array $options
     */
    public function setExchangeOptions($options)
    {
        $this->exchangeOptions = array_merge(
            $this->exchangeOptions,
            $options
        );
    }

    /**
     * @param array $options
     */
    public function setQueueOptions($options)
    {
        $this->queueOptions = array_merge(
            $this->queueOptions,
            $options
        );
    }

    /**
     * @param array $options
     */
    public function setQosOptions($options)
    {
        $this->qosOptions = array_merge(
            $this->queueOptions,
            $options
        );
	}

	/**
     * @return AMQPStreamConnection
     */
    public function getConnection()
    {
        return $this->connection;
    }

    /**
     * @return AMQPChannel
     */
    protected function getChannel()
    {
        if (null === $this->channel) {
            $this->prepareChannel();
        }
        
        return $this->channel;
    }

    protected function closeChannel()
    {
        $this->getChannel()->close();
        $this->channel = null;
    }

    /**
     * @param string $queueName
     */
    protected function queueDeclare($queueName)
    {
        $this->getChannel()->queue_declare(
            $queueName,
            $this->queueOptions['passive'],
            $this->queueOptions['durable'],
            $this->queueOptions['exclusive'],
            $this->queueOptions['auto_delete'],
            $this->queueOptions['nowait'],
            $this->queueOptions['arguments'],
            $this->queueOptions['ticket']
        );
    }

    /**
     * @param string $exchangeName
     */
    protected function exchangeDeclare($exchangeName)
    {
        $this->getChannel()->exchange_declare(
            $exchangeName,
            $this->exchangeOptions['type'],
            $this->exchangeOptions['passive'],
            $this->exchangeOptions['durable'],
            $this->exchangeOptions['auto_delete'],
            $this->exchangeOptions['internal'],
            $this->exchangeOptions['nowait'],
            $this->exchangeOptions['arguments'],
            $this->exchangeOptions['ticket']
        );
    }

    /**
     * @param string $queueName
     * @param string $exchangeName
     * @param string $routingKey
     */
    protected function queueBind($queueName, $exchangeName, $routingKey = '')
    {
        $this->getChannel()->queue_bind(
            $queueName,
            $exchangeName,
            $routingKey
        );
    }

    /**
     * @param string $queueName
     * @param string $exchangeName
     * @param string $routingKey
     */
    protected function queueUnbind($queueName, $exchangeName, $routingKey = '')
    {
        $this->getChannel()->queue_unbind(
            $queueName,
            $exchangeName,
            $routingKey
        );
    }

    protected function prepareChannel()
    {
        $this->channel = $this->getConnection()->channel();
    }

    /**
     * @param string $queue
     * @return void
     */
    protected function queueDelete($queue)
    {
        $this->getChannel()->queue_delete($queue);
    }

    protected function applyQosOptions()
    {
        $this->getChannel()->basic_qos(
            $this->qosOptions['prefetch_size'],
            $this->qosOptions['prefetch_count'],
            $this->qosOptions['global']
        );
    }
}
