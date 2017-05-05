<?php

namespace Fedot\Amqp;

use PhpAmqpLib\Message\AMQPMessage;
use React\EventLoop\Timer\TimerInterface;

abstract class HeartbeatConsumerAbstract extends ConsumerAbstract
{
    /**
     * @var TimerInterface
     */
    protected $heartbeatTimer;

    /**
     * @var int seconds
     */
    protected $heartbeatInterval = 10;

    /**
     * @var int
     */
    protected $lastProcessedMessageTime = 0;

    /**
     * @param int $heartbeatInterval
     */
    public function setHeartbeatInterval($heartbeatInterval)
    {
        $this->heartbeatInterval = $heartbeatInterval;
    }

    protected function preConsume(): void
    {
        parent::preConsume();

        $this->heartbeatTimer = $this->eventLoop->addPeriodicTimer(1, [$this, 'sendHeartbeat']);
    }

    protected function finishConsume(): void
    {
        $this->eventLoop->cancelTimer($this->heartbeatTimer);

        parent::finishConsume();
    }

    public function sendHeartbeat()
    {
        if (time() - $this->lastProcessedMessageTime >= $this->heartbeatInterval) {
            $heartbeatMessage = new AMQPMessage(
                'heartbeat-frame',
                ['delivery_mode' => 2]
            );

            $queues = $this->getQueues();
            /** @var Queue $queue */
            $queue = array_pop($queues);

            $this->getChannel()->basic_publish($heartbeatMessage, '', $queue->getName());
        }
    }

    protected function getProcessCallback(): callable
    {
        return [$this, 'processMessageWithHeartbeatCheck'];
    }

    /**
     * @param AMQPMessage $message
     */
    public function processMessageWithHeartbeatCheck(AMQPMessage $message)
    {
        if ($message->body === 'heartbeat-frame') {
            $this->messageAck($message);
        } else {
            $this->processMessage($message);
        }

        $this->lastProcessedMessageTime = time();
    }
}
