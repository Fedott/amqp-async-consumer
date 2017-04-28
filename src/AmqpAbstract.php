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

    public function __construct(
        AMQPStreamConnection $connection,
        array $exchangeOptions = null,
        array $queueOptions = null,
        array $qosOptions = null
    ) {
        $this->connection = $connection;
        $exchangeOptions !== null && $this->setExchangeOptions($exchangeOptions);
        $queueOptions !== null && $this->setQueueOptions($queueOptions);
        $qosOptions !== null && $this->setQosOptions($qosOptions);
    }

    public function setExchangeOptions(array $options): void
    {
        $this->exchangeOptions = array_merge(
            $this->exchangeOptions,
            $options
        );
    }

    public function setQueueOptions(array $options): void
    {
        $this->queueOptions = array_merge(
            $this->queueOptions,
            $options
        );
    }

    public function setQosOptions(array $options): void
    {
        $this->qosOptions = array_merge(
            $this->queueOptions,
            $options
        );
	}

    public function getConnection(): AMQPStreamConnection
    {
        return $this->connection;
    }

    protected function getChannel(): AMQPChannel
    {
        if (null === $this->channel) {
            $this->prepareChannel();
        }
        
        return $this->channel;
    }

    protected function closeChannel(): void
    {
        $this->getChannel()->close();
        $this->channel = null;
    }

    protected function queueDeclare(string $queueName): void
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

    protected function exchangeDeclare(string $exchangeName): void
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

    protected function queueBind(string $queueName, string $exchangeName, string $routingKey = ''): void
    {
        $this->getChannel()->queue_bind(
            $queueName,
            $exchangeName,
            $routingKey
        );
    }

    protected function queueUnbind(string $queueName, string $exchangeName, string $routingKey = ''): void
    {
        $this->getChannel()->queue_unbind(
            $queueName,
            $exchangeName,
            $routingKey
        );
    }

    protected function prepareChannel(): void
    {
        $this->channel = $this->getConnection()->channel();
    }

    protected function queueDelete(string $queue): void
    {
        $this->getChannel()->queue_delete($queue);
    }

    protected function applyQosOptions(): void
    {
        $this->getChannel()->basic_qos(
            $this->qosOptions['prefetch_size'],
            $this->qosOptions['prefetch_count'],
            $this->qosOptions['global']
        );
    }
}
