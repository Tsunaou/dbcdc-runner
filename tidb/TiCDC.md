# 使用 Jepsen 对 TiDB 进行测试

# 简易流程

1. 使用 TiUP 启动集群
2. 启动 TiCDC Server，并创建 changefeed
3. 初始化同步 SQL 语句
4. 删除 tidb/cdc.log
5. 启动测试

# TiDB 搭建

## 本地部署
使用 `tiup` 工具启动一个基础的 TiDB 集群，存储层由 3 个 TiKV 节点构成。
```
tiup playground v6.1.0 --db 2 --pd 3 --kv 3 --tag try-cdc-diy
```

# Jepsen TiDB 适配
由于 TiDB 适配了 MySQL 的协议，因此我们按照链接 MySQL 的方式来链接本地的 TiDB。



# TiCDC 使用

## 部署 TiDB
首先，启动 TiDB。我们这里用 `tiup` 工具来部署一个单机的伪分布式集群。

启动后，有各个组件信息如下：
```bash
CLUSTER START SUCCESSFULLY, Enjoy it ^-^
To connect TiDB: mysql --comments --host 127.0.0.1 --port 4000 -u root -p (no password)
To connect TiDB: mysql --comments --host 127.0.0.1 --port 4001 -u root -p (no password)
To view the dashboard: http://127.0.0.1:2379/dashboard
PD client endpoints: [127.0.0.1:2379 127.0.0.1:2382 127.0.0.1:2384]
To view the Prometheus: http://127.0.0.1:9090
To view the Grafana: http://127.0.0.1:3000
```
## 启动并配置 TiCDC

然后，我们启动 TiCDC。这里的 TiCDC 我们直接编译 Tiflow 源代码的中的`cdc`组件，
并在关键的位置加入一个日志输出，使得我们可以访问事务日志项。

首先，绑定 PD，为 CDC 增加一个上游（upstream）：
```bash
./bin/cdc server --pd http://127.0.0.1:2379 --data-dir /tmp/cdc_data --log-file /tmp/cdc_data/tmp.log
```
之后，我们启动一个本地的 MySQL 容器，然后为 CDC 增加一个下游（downstream）：
```bash
$ ./bin/cdc cli changefeed create --pd http://127.0.0.1:2379 --sink-uri mysql://root:123456@localhost:3306/
```
这样，我们就建立起了一个上下游服务。

## 启动 Jepsen 测试


我们首先启动一个进程

我们启动一个 Jepsen 测试后，



# 参考链接
- [tail -f 输出重定向](https://blog.csdn.net/Mrheiiow/article/details/109738702)
