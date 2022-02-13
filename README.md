# raftd
raftd 基于 raft 和 内存(后续支持 bolt) 的分布式KV数据库 基于gin框架提供http服务

## Usage

``` sh
./raftd -id node01 -haddr 127.0.0.1:8001 -raddr 127.0.0.1:8101 ~/.raftd01

./raftd -id node02 -haddr 127.0.0.1:8002 -raddr 127.0.0.1:8102 -join 127.0.0.1:8001 ~/.raftd02

./raftd -id node03 -haddr 127.0.0.1:8003 -raddr 127.0.0.1:8103 -join 127.0.0.1:8001 ~/.raftd03
```

``` sh
curl -X PUT 127.0.0.1:8001/key/test -T ./test.png

curl 127.0.0.1:8001/key/test --output 1.png
```


## TODO
- 保留 Store.m 作为 Stroe.db 的数据缓存 其实主要是用来支持 raft 集群的
- 进行快照同步的时候 直接传输这个 Stroe.m 可以有效降低网络负载(相较直接传输 整个 db文件)

> 步骤：

1. 启动的时候只make() 然后 调用 tx.ForEach() 遍历整个 db 建立缓存
2. 更新/删除 KV 的时候同步更新缓存
3. 集群同步的时候 直接使用 Store.m 进行数据传输

> 节点同步的步骤：

1. 先 fsm.db.Close() 然后 截断 data.db
2. 重新建立 db bolt.Open(...)
3. range snapshot.m 然后执行一个 Batch Update 将数据同步到 fsm.db 和 fsm.m 中
4. TODO 失败的情况

## 租约系统
系统开启时创建lease bucket
开放接口 POST createlease/ 先申请 使用雪花算法生成一个leaseId 每个leaseId 作为一个lease bucket的 subbucket
成功申请后 每个key 的形式为 leaseId:ttl 每 ttl 秒遍历一次当前bucket的所有键值 将value减一 已经是0的不变 表示过去了一秒 同时统计所有value不为0的值 当这个统计值为0的时候 删除这个桶

开放接口 POST lease/:leaseId/:key 将表单中的内容提交 raftd找到对应的桶，然后将key:[ttl(uint64 8个字节)][]byte 存进去 ttl 设置为 leaseId的ttl

TODO keepAlive()
