<?php

namespace Com\Genzouw\HadoopClients;

class HdfsClient
{
    private $requestHosts = array();
    private $requestPort = null;
    private $hdfsUser = null;
    private $requestUrl = 'http://%s:%d/webhdfs/v1%s?op=%s';

    /**
     * __construct.
     *
     * @param array  $hosts webhdfs serves
     * @param int    $port  webhdfs port
     * @param string $user  user
     */
    public function __construct(array $hosts, int $port = 50070, string $user = 'hdfs')
    {
        $this->requestHosts = $hosts;
        $this->requestPort = $port;
        $this->hdfsUser = $user;
    }

    public function fileExisted(string $hdfsFilePath)
    {
        foreach ($this->requestHosts as $host) {
            $ret = $this->doGet($host, 'GETFILESTATUS', $hdfsFilePath);
            break;
        }

        $status = json_decode($ret, true);

        return $this->isSuccess($ret) || !$status['RemoteException']['exception'] === 'FileNotFoundException';
    }

    public function fileSize(string $hdfsFilePath)
    {
        throw new Exception('Not implemented!');
    }

    public function getFileContent(string $hdfsFilePath)
    {
        throw new Exception('Not implemented!');
    }

    public function getFileToLoacl(string $hdfsFilePath, string $localFilePath)
    {
        throw new Exception('Not implemented!');
    }

    /**
     * remove filefrom hdfs.
     *
     * @param string $hdfsFilePath hdfs file path
     */
    public function removeFileFromRemote(string $hdfsFilePath)
    {
        $result = false;

        if ($this->fileExisted($hdfsFilePath)) {
            foreach ($this->requestHosts as $host) {
                $ret = $this->doDelete($host, 'DELETE', $hdfsFilePath);
                if ($this->isSuccess($ret)) {
                    $result = true;
                    break;
                }
            }
        }

        return $result;
    }

    /**
     * put data to hdfs.
     *
     * @param string $hdfsFilePath hdfs file path
     * @param string $data         putting data
     */
    public function putFileToRemote(string $hdfsFilePath, string $data)
    {
        $result = false;

        if (!$this->fileExisted($hdfsFilePath)) {
            foreach ($this->requestHosts as $host) {
                $ret = $this->doPut($host, 'CREATE', $hdfsFilePath);
                if ($this->isSuccess($ret)) {
                    $result = true;
                    break;
                }
            }
        }

        if (!$result) {
            return $result;
        }

        foreach ($this->requestHosts as $host) {
            $ret = $this->doPost($host, 'APPEND', $hdfsFilePath, gzencode($data));
            if ($this->isSuccess($ret)) {
                $result = true;
                break;
            }
        }

        return $result;
    }

    public function commonHeader(string $requestHost, string $operation, string $hdfsFilePath)
    {
        $apiEndpoint = "http://{$requestHost}:{$this->requestPort}/webhdfs/v1${hdfsFilePath}";

        $ch = curl_init();

        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
        curl_setopt($ch, CURLOPT_HTTPHEADER, array(
            'Content-Type: application/json',
        ));
        curl_setopt($ch, CURLOPT_URL, "{$apiEndpoint}?op={$operation}");

        return $ch;
    }

    public function doGet(string $requestHost, string $operation, string $hdfsFilePath)
    {
        try {
            $ch = $this->commonHeader($requestHost, $operation, $hdfsFilePath);

            // HDFS上にファイルを作成（すでに存在する場合は何もしない）
            curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'GET');
            $result = curl_exec($ch);

            return $result;
        } finally {
            if (!is_null($ch)) {
                curl_close($ch);
            }
        }
    }

    public function doPut(string $requestHost, string $operation, string $hdfsFilePath)
    {
        try {
            $ch = $this->commonHeader($requestHost, $operation, $hdfsFilePath);

            // HDFS上にファイルを作成（すでに存在する場合は何もしない）
            curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'PUT');
            $result = curl_exec($ch);

            return $result;
        } finally {
            if (!is_null($ch)) {
                curl_close($ch);
            }
        }
    }

    public function doDelete(string $requestHost, string $operation, string $hdfsFilePath)
    {
        try {
            $ch = $this->commonHeader($requestHost, $operation, $hdfsFilePath);

            // HDFS上にファイルを作成（すでに存在する場合は何もしない）
            curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'DELETE');
            $result = curl_exec($ch);

            return $result;
        } finally {
            if (!is_null($ch)) {
                curl_close($ch);
            }
        }
    }

    public function doPost(string $requestHost, string $operation, string $hdfsFilePath, string $data)
    {
        try {
            // HDFS上にファイルを追記
            $ch = $this->commonHeader($requestHost, $operation, $hdfsFilePath);

            curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'POST');
            curl_setopt($ch, CURLOPT_POST, true);
            curl_setopt(
                $ch, CURLOPT_POSTFIELDS,
                $data
            );
            $result = curl_exec($ch);

            return $result;
        } finally {
            if (!is_null($ch)) {
                curl_close($ch);
            }
        }
    }

    private function isSuccess(string $ret)
    {
        if (empty($ret)) {
            return true;
        }

        $status = json_decode($ret, true);

        return !isset($status['RemoteException']['exception']) || empty($status['RemoteException']['exception']);
    }
}
