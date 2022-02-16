# raftd
raftd 基于`raft`算法，支持数据持久化(bolt)的分布式KV数据库，可以用于简单的服务发现，基于gin框架提供http服务

## Usage

``` sh
cd cmd/raftd

go build

./raftd -id node01 -haddr 127.0.0.1:8001 -raddr 127.0.0.1:8101 ~/.raftd01

./raftd -id node02 -haddr 127.0.0.1:8002 -raddr 127.0.0.1:8102 -join 127.0.0.1:8001 ~/.raftd02

./raftd -id node03 -haddr 127.0.0.1:8003 -raddr 127.0.0.1:8103 -join 127.0.0.1:8001 ~/.raftd03
```

``` sh
curl -X PUT 127.0.0.1:8001/key/test -T ./test.png

curl 127.0.0.1:8001/key/test --output 1.png
```


> 步骤：

1. 启动的时候只make() 然后 调用 tx.ForEach() 遍历整个 db 建立缓存
2. 更新/删除 KV 的时候同步更新缓存
3. 集群同步的时候 直接使用 Store.m 进行数据传输

> 节点同步的步骤：

1. 先 fsm.db.Close() 然后 截断 data.db
2. 重新建立 db bolt.Open(...)
3. range snapshot.m 然后执行一个 Batch Update 将数据同步到 fsm.db 和 fsm.m 中
4. TODO 失败的情况

## 租约系统(可以用于服务发现)

### Usage

获取租约并续租
``` sh
# 服务1
ID=$(curl -s -XPOST 127.0.0.1:8001/lease/grant\?ttl=10\&name=/esq/node-1);while true;do curl -vv -XPOST 127.0.0.1:8001/lease/keepalive/$ID -d 'key=nodeinfo' -d 'data={"http_addr":"127.0.0.1:9001","tcp_addr":"127.0.0.1:9002","node_id":1,"weight":1}';sleep 3;done


# 服务2
ID=$(curl -s -XPOST 127.0.0.1:8001/lease/grant\?ttl=10\&name=/esq/node-2);while true;do curl -vv -XPOST 127.0.0.1:8001/lease/keepalive/$ID -d 'key=nodeinfo1' -d 'data={"http_addr":"127.0.0.1:9003","tcp_addr":"127.0.0.1:9004","node_id":2,"weight":2}';sleep 3;done
```

获取键值对

``` sh
curl -s -XPOST 127.0.0.1:8001/lease/kv/nodeinfo -d 'prefix=/esq/node'
```

### TODO
- Revoke API
- TimeToLive API

