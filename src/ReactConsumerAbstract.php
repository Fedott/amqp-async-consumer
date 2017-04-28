<?php

namespace Fedot\Amqp;

use PhpAmqpLib\Wire\IO\SocketIO;
use React\EventLoop\LoopInterface;
use React\EventLoop\Timer\TimerInterface;

abstract class ReactConsumerAbstract extends ConsumerAbstract
{
    /**
     * @var LoopInterface
     */
    protected $eventLoop;

    /**
     * @var bool
     */
    protected $isProcessing = false;

    /**
     * @var TimerInterface
     */
    protected $checkTimer;

    /**
     * @param LoopInterface $eventLoop
     */
    public function setEventLoop(LoopInterface $eventLoop)
    {
        $this->eventLoop = $eventLoop;
    }

    /**
     * @return SocketIO|resource
     */
    public function getSocket()
    {
        return $this->getChannel()->getConnection()->getSocket();
    }

    public function consume()
    {
        $this->preConsume();
        $this->prepareQueues();

        $this->preReadStream();
        $this->startReadStream();
    }

    protected function preReadStream()
    {

    }

    protected function startReadStream()
    {
        /** @noinspection PhpParamsInspection disabled due to invalid phpdoc in vendor code */
        $this->eventLoop->addReadStream($this->getSocket(), [$this, 'process']);

        /** @noinspection PhpParamsInspection disabled due to invalid phpdoc in vendor code */
        $this->checkTimer = $this->eventLoop->addPeriodicTimer(0.1, [$this, 'checkNotAckMessages']);
    }

    public function consumeCancel()
    {
        /** @noinspection PhpParamsInspection disabled due to invalid phpdoc in vendor code */
        $this->eventLoop->removeReadStream($this->getSocket());
        $this->eventLoop->cancelTimer($this->checkTimer);

        $this->finishConsume();

        $this->getChannel()->close();
    }

    public function process()
    {
        $this->isProcessing = true;

        if (count($this->getChannel()->callbacks)) {
            $this->getChannel()->wait(null, true);

            $this->postProcess();
        }

        $this->isProcessing = false;

        $this->stopIfNeeded();
    }

    protected function stopIfNeeded()
    {
        if ($this->stop) {
            $this->consumeCancel();
        }
    }

    public function stop()
    {
        if (!$this->isProcessing) {
            $this->consumeCancel();

        } else {
            $this->stop = true;
        }
    }

    /**
     * Workaround for unacked messages on socket after delivered all messages from amqp server
     */
    public function checkNotAckMessages()
    {
        if (!$this->isProcessing && $this->existsNotAckMessages()) {
            $this->process();
        }
    }

    /**
     * @return bool
     */
    public function existsNotAckMessages()
    {
        $socketInfo = socket_get_status($this->getSocket());

        return $socketInfo['unread_bytes'] > 0;
    }
}
