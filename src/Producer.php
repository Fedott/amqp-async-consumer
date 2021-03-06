<?php

namespace Fedot\Amqp;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

class Producer extends AmqpAbstract
{
    /**
     * @var string
     */
    protected $exchange;

    /**
     * @var Queue[]
     */
    protected $defaultQueues = [];

    /**
     * @var bool
     */
    protected $channelReady = false;

    public function __construct(
        AMQPStreamConnection $connection,
        string $exchange,
        array $defaultQueues = [],
        array $exchangeOptions = null,
        array $queueOptions = null,
        array $qosOptions = null
    ) {
        $this->exchange = $exchange;
        $this->setDefaultQueues($defaultQueues);

        parent::__construct($connection, $exchangeOptions, $queueOptions, $qosOptions);
    }

    public function getExchange(): string
    {
        return $this->exchange;
    }

    public function setExchange(string $exchange): void
    {
        $this->exchange = $exchange;
    }

    /**
     * @return Queue[]
     */
    public function getDefaultQueues()
    {
        return $this->defaultQueues;
    }

    /**
     * @param Queue[]|string[][]|string[] $defaultQueues
     */
    public function setDefaultQueues(array $defaultQueues)
    {
        $queues = [];
        foreach ($defaultQueues as $queue) {
            if ($queue instanceof Queue) {
                $queues[] = $queue;
            } elseif (is_array($queue)) {
                $queues[] = new Queue($queue[0], $queue[1] ?? null, $queue[2] ?? '');
            } else {
                $queues[] = new Queue($queue);
            }
        }

        $this->defaultQueues = $queues;
    }

    public function publish(AMQPMessage $message, string $routingKey = ''): void
    {
        if (!$this->channelReady) {
            $this->prepareChannel();
        }

        try {
            $this->getChannel()->basic_publish($message, $this->getExchange(), $routingKey);
        } catch (\RuntimeException $e) {
            $this->channelReady = false;
            $this->publish($message, $routingKey);
        }
    }

    protected function prepareChannel(): void
    {
        if (null !== $this->channel) {
            $this->getConnection()->reconnect();
        }

        $this->channel = $this->getConnection()->channel();

        $this->initChannel();

        $this->channelReady = true;
    }

    protected function initChannel(): void
    {
        $this->exchangeDeclare($this->getExchange());
        if (!empty($this->getDefaultQueues())) {
            foreach ($this->getDefaultQueues() as $queue) {
                $this->queueDeclare($queue->getName());
                $this->queueBind($queue->getName(), $this->getExchange(), $queue->getRoutingKey());
            }
        }
    }
}
