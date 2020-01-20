<?php
namespace isaactw;

class MySSDB extends SSDB
{
    public function __construct($host, $port, $timeoutMs = 2000)
    {
        parent::__construct($host, $port, $timeoutMs);
        $this->easy();
    }
}
