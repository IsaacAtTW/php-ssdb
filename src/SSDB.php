<?php
namespace isaactw;

// Depricated, use SimpleSSDB instead!
class SSDB
{
    private $debug = false;
    public $sock = null;
    private $isClosed = false;
    private $recvBuf = '';
    private $isEasy = false;
    public $lastResp = null;
    public function __construct($host, $port, $timeoutMs = 2000)
    {
        $timeoutF = (float)$timeoutMs/1000;
        $this->sock = @stream_socket_client("$host:$port", $errno, $errstr, $timeoutF);
        if (!$this->sock) {
            throw new SSDBException("$errno: $errstr");
        }
        $timeoutSec = intval($timeoutMs/1000);
        $timeoutUsec = ($timeoutMs - $timeoutSec * 1000) * 1000;
        @stream_set_timeout($this->sock, $timeoutSec, $timeoutUsec);
        if (function_exists('stream_set_chunk_size')) {
            @stream_set_chunk_size($this->sock, 1024 * 1024);
        }
    }
    
    public function setTimeout($timeoutMs)
    {
        $timeoutSec = intval($timeoutMs/1000);
        $timeoutUsec = ($timeoutMs - $timeoutSec * 1000) * 1000;
        @stream_set_timeout($this->sock, $timeoutSec, $timeoutUsec);
    }
    
    /**
     * After this method invoked with yesno=true, all requesting methods
     * will not return a SSDB_Response object.
     * And some certain methods like get/zget will return false
     * when response is not ok(not_found, etc)
     */
    public function isEasy()
    {
        $this->isEasy = true;
    }
    public function close()
    {
        if (!$this->_closed) {
            @fclose($this->sock);
            $this->isClosed = true;
            $this->sock = null;
        }
    }
    public function closed()
    {
        return $this->isClosed;
    }
    private $batchMode = false;
    private $batchCmds = array();
    public function batch()
    {
        $this->batchMode = true;
        $this->batchCmds = array();
        return $this;
    }
    public function multi()
    {
        return $this->batch();
    }
    public function exec()
    {
        $ret = array();
        foreach ($this->batchCmds as $op) {
            list($cmd, $params) = $op;
            $this->sendReq($cmd, $params);
        }
        foreach ($this->batchCmds as $op) {
            list($cmd, $params) = $op;
            $resp = $this->recvResp($cmd, $params);
            $resp = $this->checkEasyResp($cmd, $resp);
            $ret[] = $resp;
        }
        $this->batchMode = false;
        $this->batchCmds = array();
        return $ret;
    }
    
    public function request()
    {
        $args = func_get_args();
        $cmd = array_shift($args);
        return $this->__call($cmd, $args);
    }
    
    private $asyncAuthPassword = null;
    
    public function auth($password)
    {
        $this->asyncAuthPassword = $password;
        return null;
    }
    public function __call($cmd, $params = array())
    {
        $cmd = strtolower($cmd);
        if ($this->asyncAuthPassword !== null) {
            $pass = $this->asyncAuthPassword;
            $this->asyncAuthPassword = null;
            $auth = $this->__call('auth', array($pass));
            if ($auth !== true) {
                throw new \Exception("Authentication failed");
            }
        }
        if ($this->batchMode) {
            $this->batchCmds[] = array($cmd, $params);
            return $this;
        }
        try {
            if ($this->sendReq($cmd, $params) === false) {
                $resp = new SSDBResponse('error', 'send error');
            } else {
                $resp = $this->recvResp($cmd, $params);
            }
        } catch (SSDBException $e) {
            if ($this->_easy) {
                throw $e;
            } else {
                $resp = new SSDBResponse('error', $e->getMessage());
            }
        }
        if ($resp->code == 'noauth') {
            $msg = $resp->message;
            throw new \Exception($msg);
        }
        
        $resp = $this->checkEasyResp($cmd, $resp);
        return $resp;
    }
    private function checkEasyResp($cmd, $resp)
    {
        $this->last_resp = $resp;
        if ($this->isEasy) {
            if ($resp->not_found()) {
                return null;
            } elseif (!$resp->ok() && !is_array($resp->data)) {
                return false;
            } else {
                return $resp->data;
            }
        } else {
            $resp->cmd = $cmd;
            return $resp;
        }
    }
    public function multiSet($kvs = array())
    {
        $args = array();
        foreach ($kvs as $k => $v) {
            $args[] = $k;
            $args[] = $v;
        }
        return $this->__call(__FUNCTION__, $args);
    }
    public function multiHset($name, $kvs = array())
    {
        $args = array($name);
        foreach ($kvs as $k => $v) {
            $args[] = $k;
            $args[] = $v;
        }
        return $this->__call(__FUNCTION__, $args);
    }
    public function multiZset($name, $kvs = array())
    {
        $args = array($name);
        foreach ($kvs as $k => $v) {
            $args[] = $k;
            $args[] = $v;
        }
        return $this->__call(__FUNCTION__, $args);
    }
    public function incr($key, $val = 1)
    {
        $args = func_get_args();
        return $this->__call(__FUNCTION__, $args);
    }
    public function decr($key, $val = 1)
    {
        $args = func_get_args();
        return $this->__call(__FUNCTION__, $args);
    }
    public function zincr($name, $key, $score = 1)
    {
        $args = func_get_args();
        return $this->__call(__FUNCTION__, $args);
    }
    public function zdecr($name, $key, $score = 1)
    {
        $args = func_get_args();
        return $this->__call(__FUNCTION__, $args);
    }
    public function zadd($key, $score, $value)
    {
        $args = array($key, $value, $score);
        return $this->__call('zset', $args);
    }
    public function zRevRank($name, $key)
    {
        $args = func_get_args();
        return $this->__call("zrrank", $args);
    }
    public function zRevRange($name, $offset, $limit)
    {
        $args = func_get_args();
        return $this->__call("zrrange", $args);
    }
    public function hincr($name, $key, $val = 1)
    {
        $args = func_get_args();
        return $this->__call(__FUNCTION__, $args);
    }
    public function hdecr($name, $key, $val = 1)
    {
        $args = func_get_args();
        return $this->__call(__FUNCTION__, $args);
    }
    private function sendReq($cmd, $params)
    {
        $req = array($cmd);
        foreach ($params as $p) {
            if (is_array($p)) {
                $req = array_merge($req, $p);
            } else {
                $req[] = $p;
            }
        }
        return $this->send($req);
    }
    private function recvResp($cmd, $params)
    {
        $resp = $this->recv();
        if ($resp === false) {
            return new SSDBResponse('error', 'Unknown error');
        } elseif (!$resp) {
            return new SSDBResponse('disconnected', 'Connection closed');
        }
        if ($resp[0] == 'noauth') {
            $errmsg = isset($resp[1])? $resp[1] : '';
            return new SSDBResponse($resp[0], $errmsg);
        }
        switch ($cmd) {
            case 'dbsize':
            case 'ping':
            case 'qset':
            case 'getbit':
            case 'setbit':
            case 'countbit':
            case 'strlen':
            case 'set':
            case 'setx':
            case 'setnx':
            case 'zset':
            case 'hset':
            case 'qpush':
            case 'qpush_front':
            case 'qpush_back':
            case 'qtrim_front':
            case 'qtrim_back':
            case 'del':
            case 'zdel':
            case 'hdel':
            case 'hsize':
            case 'zsize':
            case 'qsize':
            case 'hclear':
            case 'zclear':
            case 'qclear':
            case 'multiSet':
            case 'multiDel':
            case 'multiHset':
            case 'multiHdel':
            case 'multiZset':
            case 'multiZdel':
            case 'incr':
            case 'decr':
            case 'zincr':
            case 'zdecr':
            case 'hincr':
            case 'hdecr':
            case 'zget':
            case 'zrank':
            case 'zrrank':
            case 'zcount':
            case 'zsum':
            case 'zremrangebyrank':
            case 'zremrangebyscore':
            case 'ttl':
            case 'expire':
                if ($resp[0] == 'ok') {
                    $val = isset($resp[1])? intval($resp[1]) : 0;
                    return new SSDBResponse($resp[0], $val);
                } else {
                    $errmsg = isset($resp[1])? $resp[1] : '';
                    return new SSDBResponse($resp[0], $errmsg);
                }
            case 'zavg':
                if ($resp[0] == 'ok') {
                    $val = isset($resp[1])? floatval($resp[1]) : (float)0;
                    return new SSDBResponse($resp[0], $val);
                } else {
                    $errmsg = isset($resp[1])? $resp[1] : '';
                    return new SSDBResponse($resp[0], $errmsg);
                }
            case 'get':
            case 'substr':
            case 'getset':
            case 'hget':
            case 'qget':
            case 'qfront':
            case 'qback':
                if ($resp[0] == 'ok') {
                    if (count($resp) == 2) {
                        return new SSDBResponse('ok', $resp[1]);
                    } else {
                        return new SSDBResponse('server_error', 'Invalid response');
                    }
                } else {
                    $errmsg = isset($resp[1])? $resp[1] : '';
                    return new SSDBResponse($resp[0], $errmsg);
                }
                break;
            case 'qpop':
            case 'qpop_front':
            case 'qpop_back':
                if ($resp[0] == 'ok') {
                    $size = 1;
                    if (isset($params[1])) {
                        $size = intval($params[1]);
                    }
                    if ($size <= 1) {
                        if (count($resp) == 2) {
                            return new SSDBResponse('ok', $resp[1]);
                        } else {
                            return new SSDBResponse('server_error', 'Invalid response');
                        }
                    } else {
                        $data = array_slice($resp, 1);
                        return new SSDBResponse('ok', $data);
                    }
                } else {
                    $errmsg = isset($resp[1])? $resp[1] : '';
                    return new SSDBResponse($resp[0], $errmsg);
                }
                break;
            case 'keys':
            case 'zkeys':
            case 'hkeys':
            case 'hlist':
            case 'zlist':
            case 'qslice':
                if ($resp[0] == 'ok') {
                    $data = array();
                    if ($resp[0] == 'ok') {
                        $data = array_slice($resp, 1);
                    }
                    return new SSDBResponse($resp[0], $data);
                } else {
                    $errmsg = isset($resp[1])? $resp[1] : '';
                    return new SSDBResponse($resp[0], $errmsg);
                }
            case 'auth':
            case 'exists':
            case 'hexists':
            case 'zexists':
                if ($resp[0] == 'ok') {
                    if (count($resp) == 2) {
                        return new SSDBResponse('ok', (bool)$resp[1]);
                    } else {
                        return new SSDBResponse('server_error', 'Invalid response');
                    }
                } else {
                    $errmsg = isset($resp[1])? $resp[1] : '';
                    return new SSDBResponse($resp[0], $errmsg);
                }
                break;
            case 'multiExists':
            case 'multiHexists':
            case 'multiZexists':
                if ($resp[0] == 'ok') {
                    if (count($resp) % 2 == 1) {
                        $data = array();
                        for ($i=1; $i<count($resp); $i+=2) {
                            $data[$resp[$i]] = (bool)$resp[$i + 1];
                        }
                        return new SSDBResponse('ok', $data);
                    } else {
                        return new SSDBResponse('server_error', 'Invalid response');
                    }
                } else {
                    $errmsg = isset($resp[1])? $resp[1] : '';
                    return new SSDBResponse($resp[0], $errmsg);
                }
                break;
            case 'scan':
            case 'rscan':
            case 'zscan':
            case 'zrscan':
            case 'zrange':
            case 'zrrange':
            case 'hscan':
            case 'hrscan':
            case 'hgetall':
            case 'multiHsize':
            case 'multiZsize':
            case 'multiGet':
            case 'multiHget':
            case 'multiZget':
            case 'zpopFront':
            case 'zpopBack':
                if ($resp[0] == 'ok') {
                    if (count($resp) % 2 == 1) {
                        $data = array();
                        for ($i=1; $i<count($resp); $i+=2) {
                            if ($cmd[0] == 'z') {
                                $data[$resp[$i]] = intval($resp[$i + 1]);
                            } else {
                                $data[$resp[$i]] = $resp[$i + 1];
                            }
                        }
                        return new SSDBResponse('ok', $data);
                    } else {
                        return new SSDBResponse('server_error', 'Invalid response');
                    }
                } else {
                    $errmsg = isset($resp[1])? $resp[1] : '';
                    return new SSDBResponse($resp[0], $errmsg);
                }
                break;
            default:
                return new SSDBResponse($resp[0], array_slice($resp, 1));
        }
        return new SSDBResponse('error', 'Unknown command: $cmd');
    }
    public function send($data)
    {
        $ps = array();
        foreach ($data as $p) {
            $ps[] = strlen($p);
            $ps[] = $p;
        }
        $s = join("\n", $ps) . "\n\n";
        if ($this->debug) {
            echo '> ' . str_replace(array("\r", "\n"), array('\r', '\n'), $s) . "\n";
        }
        try {
            while (true) {
                $ret = @fwrite($this->sock, $s);
                if ($ret === false || $ret === 0) {
                    $this->close();
                    throw new SSDBException('Connection lost');
                }
                $s = substr($s, $ret);
                if (strlen($s) == 0) {
                    break;
                }
                @fflush($this->sock);
            }
        } catch (\Exception $e) {
            $this->close();
            throw new SSDBException($e->getMessage());
        }
        return $ret;
    }
    public function recv()
    {
        $this->step = self::STEP_SIZE;
        while (true) {
            $ret = $this->parse();
            if ($ret === null) {
                try {
                    $data = @fread($this->sock, 1024 * 1024);
                    if ($this->debug) {
                        echo '< ' . str_replace(array("\r", "\n"), array('\r', '\n'), $data) . "\n";
                    }
                } catch (\Exception $e) {
                    $data = '';
                }
                if ($data === false || $data === '') {
                    if (feof($this->sock)) {
                        $this->close();
                        throw new SSDBException('Connection lost');
                    } else {
                        throw new SSDBTimeoutException('Connection timeout');
                    }
                }
                $this->recvBuf .= $data;
#               echo "read " . strlen($data) . " total: " . strlen($this->recvBuf) . "\n";
            } else {
                return $ret;
            }
        }
    }
    const STEP_SIZE = 0;
    const STEP_DATA = 1;
    public $resp = array();
    public $step;
    public $blockSize;
    private function parse()
    {
        $spos = 0;
        $epos = 0;
        $bufSize = strlen($this->recvBuf);
        // performance issue for large reponse
        //$this->recvBuf = ltrim($this->recvBuf);
        while (true) {
            $spos = $epos;
            if ($this->step === self::STEP_SIZE) {
                $epos = strpos($this->recvBuf, "\n", $spos);
                if ($epos === false) {
                    break;
                }
                $epos += 1;
                $line = substr($this->recvBuf, $spos, $epos - $spos);
                $spos = $epos;
                $line = trim($line);
                if (strlen($line) == 0) { // head end
                    $this->recvBuf = substr($this->recvBuf, $spos);
                    $ret = $this->resp;
                    $this->resp = array();
                    return $ret;
                }
                $this->blockSize = intval($line);
                $this->step = self::STEP_DATA;
            }
            if ($this->step === self::STEP_DATA) {
                $epos = $spos + $this->blockSize;
                if ($epos <= $bufSize) {
                    $n = strpos($this->recvBuf, "\n", $epos);
                    if ($n !== false) {
                        $data = substr($this->recvBuf, $spos, $epos - $spos);
                        $this->resp[] = $data;
                        $epos = $n + 1;
                        $this->step = self::STEP_SIZE;
                        continue;
                    }
                }
                break;
            }
        }
        // packet not ready
        if ($spos > 0) {
            $this->recvBuf = substr($this->recvBuf, $spos);
        }
        return null;
    }
}
