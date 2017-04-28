<?php

namespace Fedot\Amqp;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\IO\SocketIO;
use React\EventLoop\LoopInterface;
use React\EventLoop\Timer\TimerInterface;

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
    /**
     * @var LoopInterface
     */
    protected $eventLoop;
    /**
     * @var TimerInterface
     */
    protected $checkTimer;
    /**
     * @var bool
     */
    protected $isProcessing = false;

    public function __construct(
        LoopInterface $eventLoop,
        AMQPStreamConnection $connection,
        array $queues,
        array $exchangeOptions = null,
        array $queueOptions = null,
        array $qosOptions = null
    ) {
        $this->eventLoop = $eventLoop;
        $this->setQueues($queues);

        parent::__construct($connection, $exchangeOptions, $queueOptions, $qosOptions);
    }

    public function stop(): void
    {
        if (!$this->isProcessing) {
            $this->consumeCancel();
        } else {
            $this->stop = true;
        }
    }

    /**
     * @param Queue[]|\string[]|string[][] $queues
     */
    public function setQueues(array $queues): void
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

    public function setEventLoop(LoopInterface $eventLoop): void
    {
        $this->eventLoop = $eventLoop;
    }

    public function consumeCancel(): void
    {
        /** @noinspection PhpParamsInspection disabled due to invalid phpdoc in vendor code */
        $this->eventLoop->removeReadStream($this->getSocket());
        $this->eventLoop->cancelTimer($this->checkTimer);

        $this->finishConsume();

        $this->getChannel()->close();
    }

    /**
     * @return SocketIO|resource
     */
    public function getSocket()
    {
        return $this->getChannel()->getConnection()->getSocket();
    }

    public function process(): void
    {
        $this->isProcessing = true;

        if (count($this->getChannel()->callbacks)) {
            $this->getChannel()->wait(null, true);

            $this->postProcess();
        }

        $this->isProcessing = false;

        $this->stopIfNeeded();
    }

    /**
     * Workaround for unacked messages on socket after delivered all messages from amqp server
     */
    public function checkNotAckMessages(): void
    {
        if (!$this->isProcessing && $this->existsNotAckMessages()) {
            $this->process();
        }
    }

    public function existsNotAckMessages(): bool
    {
        $socketInfo = socket_get_status($this->getSocket());

        return $socketInfo['unread_bytes'] > 0;
    }

    protected function addQueue(Queue $queue): void
    {
        $this->queues[$queue->getHash()] = $queue;
    }

    protected function removeQueue(Queue $queue): void
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

    public function consume(): void
    {
        $this->preConsume();
        $this->prepareQueues();

        $this->preReadStream();
        $this->startReadStream();
    }

    protected function preConsume(): void
    {
        $this->applyQosOptions();
    }

    protected function postProcess(): void {}

    protected function finishConsume(): void {}

    protected function prepareQueues(): void
    {
        foreach ($this->getQueues() as $queue) {
            $this->prepareQueue($queue);
        }
    }

    protected function prepareQueue(Queue $queue): void
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

    protected function finishQueue(Queue $queue): void {}

    protected function messageAck(AMQPMessage $message): void
    {
        $this->getChannel()->basic_ack(
            $message->delivery_info['delivery_tag']
        );
    }

    protected function messageNack(AMQPMessage $message, bool $requeue = false): void
    {
        $this->getChannel()->basic_nack(
            $message->delivery_info['delivery_tag'],
            false,
            $requeue
        );
    }

    abstract public function processMessage(AMQPMessage $message): void;

    protected function getProcessCallback(): callable
    {
        return [$this, 'processMessage'];
    }

    protected function preReadStream(): void {}

    protected function startReadStream(): void
    {
        /** @noinspection PhpParamsInspection disabled due to invalid phpdoc in vendor code */
        $this->eventLoop->addReadStream($this->getSocket(), [$this, 'process']);

        /** @noinspection PhpParamsInspection disabled due to invalid phpdoc in vendor code */
        $this->checkTimer = $this->eventLoop->addPeriodicTimer(0.1, [$this, 'checkNotAckMessages']);
    }

    protected function stopIfNeeded(): void
    {
        if ($this->stop) {
            $this->consumeCancel();
        }
    }
}
