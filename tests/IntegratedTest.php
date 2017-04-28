<?php declare(strict_types=1);

use Fedot\Amqp\ConsumerAbstract;
use Fedot\Amqp\ProducerAbstract;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\TestCase;
use React\EventLoop\LoopInterface;
use React\EventLoop\StreamSelectLoop;

class IntegratedTest extends TestCase
{
    public function testProduceConsume()
    {
        $eventLoop = new StreamSelectLoop();
        $connection = new AMQPStreamConnection("localhost", "5672", "guest", "guest", "/test/");

        $producer = new ProducerAbstract();
        $producer->setConnection($connection);
        $producer->setExchange('test-exchange');
        $producer->setDefaultQueues([new \Fedot\Amqp\Queue('test-queue')]);
        $consumer = new class extends ConsumerAbstract
        {
            public $consumed = 0;

            public function processMessage(AMQPMessage $message): void
            {
                $this->consumed++;
                $this->messageAck($message);
            }

            public function consumeCancel(): void
            {
                $this->queueDelete('test-queue');

                parent::consumeCancel();
            }
        };
        $consumer->setConnection($connection);
        $consumer->setEventLoop($eventLoop);
        $consumer->setQueues(['test-queue']);

        $message1 = new AMQPMessage('message first');
        $producer->publish($message1);

        $message2 = new AMQPMessage('message two');
        $producer->publish($message2);

        $consumer->consume();

        $this->runEventLoop($eventLoop);

        $consumer->stop();

        $this->assertEquals(2, $consumer->consumed);
    }

    public function runEventLoop(LoopInterface $eventLoop): void
    {
        for ($i = 0; $i < 5; $i++) {
            $eventLoop->tick();
            usleep(50000);
        }
    }
}
