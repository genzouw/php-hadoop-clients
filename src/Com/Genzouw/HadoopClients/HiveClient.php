<?php

namespace Com\Genzouw\HadoopClients;

/**
 * Thrift(HiveServer2)サーバへの接続用クライアントクラス.
 *
 * @version $id$
 *
 * @copyright Copyright © LOCKON CO.,LTD. All Rights Reserved.
 * @author toshiak wakabayashi <toshiaki_wakabayashi@lockon.co.jp>
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

        $hiveQuery = $this->hive->query($sql);

        if (!is_null($hiveQuery)) {
            $hiveQuery->wait();
        }

        return $hiveQuery;
    }

    /**
     * queryAll.
     *
     * @param string $sqls 複数クエリ(セミコロン区切りを想定)
     */
    public function queryAll(string $sqls)
    {
        if (!$this->isConnected) {
            $this->connect();
        }

        foreach (explode(';', $sqls) as $sql) {
            if (empty(trim($sql))) {
                continue;
            }
            $hiveQuery = $this->hive->query($sql);
        }

        if (!is_null($hiveQuery)) {
            $hiveQuery->wait();
        }

        return $hiveQuery;
    }
}
