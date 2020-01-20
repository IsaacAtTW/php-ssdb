<?php
namespace isaactw;

class SSDBResponse
{
    public $cmd;
    public $code;
    public $data = null;
    public $message;
    public function __construct($code = 'ok', $dataOrMessage = null)
    {
        $this->code = $code;
        if ($code == 'ok') {
            $this->data = $dataOrMessage;
        } else {
            $this->message = $dataOrMessage;
        }
    }
    public function __toString()
    {
        if ($this->code == 'ok') {
            $s = $this->data === null? '' : json_encode($this->data);
        } else {
            $s = $this->message;
        }
        return sprintf('%-13s %12s %s', $this->cmd, $this->code, $s);
    }
    public function ok()
    {
        return $this->code == 'ok';
    }
    public function notFound()
    {
        return $this->code == 'not_found';
    }
}
