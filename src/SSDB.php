<?php
namespace isaactw;

class SSDB extends SSDBBase
{
    public function __construct($host, $port, $timeoutMs = 2000)
    {
        parent::__construct($host, $port, $timeoutMs);
        $this->easy();
    }
}
