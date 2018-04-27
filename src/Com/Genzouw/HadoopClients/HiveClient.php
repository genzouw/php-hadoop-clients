<?php

namespace Com\Genzouw\HadoopClients;

/**
 * Thrift(HiveServer2)サーバへの接続用クライアントクラス.
 *
 * @version $id$
 */
class HiveClient
{
    private $isConnected = false;

    /**
     * コンストラクタ
     *
     * @param string $host        hive host
     * @param int    $port        hive port
     * @param string $dbname      hive dbname
     * @param int    $sendTimeout 送信タイムアウト(ms)
     */
    public function __construct(
        string $host, int $port, string $dbname, int $sendTimeout
    ) {
        $this->dbname = $dbname;

        $this->hive = new \ThriftSQL\Hive($host, $port, '', '', $sendTimeout);

        $this->isConnected = false;
    }

    /**
     * サーバへ接続する.
     */
    public function connect()
    {
        $this->hive
            ->connect()
            ->query("use {$this->dbname}")->wait();

        $this->isConnected = true;
    }

    /**
     * Hive Queryを実行する.
     *
     * @param string $sql クエリ
     *
     * @return クエリ実行結果
     */
    public function query(string $sql)
    {
        if (!$this->isConnected) {
            $this->connect();
        }

        foreach (explode(';', $sql) as $s) {
            if (empty(trim($s))) {
                continue;
            }
            $hiveQuery = $this->hive->query($s);
            $hiveQuery->wait();
        }

        return $hiveQuery;
    }
}
