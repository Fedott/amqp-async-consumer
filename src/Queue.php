<?php

namespace Fedot\Amqp;

class Queue
{
    /**
     * @var string
     */
    protected $name;

    /**
     * @var string
     */
    protected $exchange;

    /**
     * @var string
     */
    protected $routingKey;

    /**
     * @param string      $name
     * @param null|string $exchange
     * @param string $routingKey
     */
    public function __construct($name, $exchange = null, $routingKey = '')
    {
        $this->name     = $name;
        $this->exchange = $exchange;
        $this->routingKey = $routingKey;
    }

    /**
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @return string|null
     */
    public function getExchange()
    {
        return $this->exchange;
    }

    /**
     * @return string
     */
    public function getRoutingKey()
    {
        return $this->routingKey;
    }

    /**
     * @return string
     */
    public function getHash()
    {
        return sha1($this->getName().$this->getExchange().$this->getRoutingKey());
    }
}
