<?php

namespace Fedot\Amqp;

use PhpAmqpLib\Message\AMQPMessage;

abstract class ConsumerAbstract extends AmqpAbstract
{
    /**
     * @var bool
     */
    protected $stop = false;

    /**
     * @var Queue[]
     */
    protected $queues = [];

    public function stop()
    {
        $this->stop = true;
    }

    /**
     * @param Queue[]|\string[] $queues
     */
    public function setQueues(array $queues)
    {
        $this->queues = [];
        foreach ($queues as $queue) {
            if (!$queue instanceof Queue) {
                if (is_array($queue)) {
                    $queue = new Queue($queue[0], $queue[1]);
                } else {
                    $queue = new Queue($queue);
                }
            }
            $this->queues[$queue->getHash()] = $queue;
        }
    }

    protected function addQueue(Queue $queue)
    {
        $this->queues[$queue->getHash()] = $queue;
    }

    protected function removeQueue(Queue $queue)
    {
        unset($this->queues[$queue->getHash()]);
        $this->finishQueue($queue);
    }

    /**
     * @return Queue[]
     */
    public function getQueues()
    {
        return $this->queues;
    }

    public function consume()
    {
        $this->preConsume();

        $this->prepareQueues();

        while (count($this->getChannel()->callbacks)) {
            if ($this->stop) {
                break;
            }

            $this->getChannel()->wait();

            $this->postProcess();
        }

        $this->finishConsume();

        $this->getChannel()->close();
    }

    protected function preConsume()
    {
        $this->applyQosOptions();
    }

    protected function postProcess() {}

    protected function finishConsume() {}

    protected function prepareQueues()
    {
        foreach ($this->getQueues() as $queue) {
            $this->prepareQueue($queue);
        }
    }

    protected function prepareQueue(Queue $queue)
    {
        $this->queueDeclare($queue->getName());

        if (null !== $queue->getExchange()) {
            $this->exchangeDeclare($queue->getExchange());

            $this->queueBind($queue->getName(), $queue->getExchange());
        }

        $this->getChannel()->basic_consume(
            $queue->getName(),
            '',
            false,
            false,
            false,
            false,
            $this->getProcessCallback()
        );
    }

    /**
     * @param Queue $queue
     */
    protected function finishQueue(Queue $queue) {}

    /**
     * @param AMQPMessage $message
     */
    protected function messageAck(AMQPMessage $message)
    {
        $this->getChannel()->basic_ack(
            $message->delivery_info['delivery_tag']
        );
    }

    /**
     * @param AMQPMessage $message
     * @return void
     */
    abstract public function processMessage(AMQPMessage $message);

    /**
     * @return callable
     */
    protected function getProcessCallback()
    {
        return [$this, 'processMessage'];
    }
}
