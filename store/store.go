package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/impact-eintr/bolt"
	"github.com/impact-eintr/lsmdb"
	"github.com/impact-eintr/raftd/utils"
)

const (
	DefaultKVBucketName    = "impact-eintr"
	DefaultLeaseBucketName = "raftd-lease-system"
)

var (
	// ErrNotLeader is returned when a node attempts to execute a leader-only
	// operation.
	ErrNotLeader = errors.New("not leader")

	// ErrOpenTimeout is returned when the Store does not apply its initial
	// logs within the specified time.
	ErrOpenTimeout = errors.New("timeout waiting for initial logs application")

	ErrLeaseNotFound = errors.New("can not find this Lease")
	ErrLeaseExpire   = errors.New("this Lease has been expired")
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	applyTimeout        = 10 * time.Second
	openTimeout         = 120 * time.Second
	leaderWaitDelay     = 100 * time.Millisecond
	appliedWaitDelay    = 100 * time.Millisecond
)

type command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value []byte `json:"value,omitempty"`

	LeaseId uint64 `json:"leaseId,omitempty"`
	Name    string `json:"name,omitempty"`
	TTL     int    `json:"ttl,omitempty"`
}

type ConsistencyLevel int

const (
	Default ConsistencyLevel = iota
	Stale
	Consistent
)

type WaitGroupWrapper struct {
	sync.WaitGroup
}

func (w *WaitGroupWrapper) Wrap(cb func()) {
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}

type Store struct {
	RaftDir  string
	RaftBind string

	// 保护m的锁
	mu  sync.RWMutex
	m   map[string][]byte // 键值对缓存 用内存实现
	db  *bolt.DB          // 键值对 用 impact-eintr/bolt 实现
	db2 *lsmdb.DB

	raft *raft.Raft // 一致性机制

	// 租约系统相关实现
	snowflake *utils.Snowflake
	wg        WaitGroupWrapper
	leases    map[uint64][]string // LeaseId:[]key

	logger *log.Logger
}

func New(bindID int64) *Store {
	sn, err := utils.NewSnowflake(bindID)
	if err != nil {
		panic(err)
	}
	return &Store{
		m:         make(map[string][]byte),
		leases:    make(map[uint64][]string),
		snowflake: sn,
		logger:    log.New(os.Stderr, "[store] ", log.LstdFlags),
	}
}

func (s *Store) LeaderID() (string, error) {
	addr := s.LeaderAddr()
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("failed to get raft configuration: %v", err)
		return "", err
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.Address == raft.ServerAddress(addr) {
			return string(srv.ID), nil
		}
	}
	return "", nil
}

func (s *Store) LeaderAddr() string {
	return string(s.raft.Leader())
}

func (s *Store) LeaderAPIAddr() string {
	id, err := s.LeaderID()
	if err != nil {
		return ""
	}

	addr, err := s.GetMeta(id)
	if err != nil {
		return ""
	}

	return string(addr)
}

func (s *Store) Open(enableSingle bool, localID string) error {
	// 配置数据存储
	db, err := bolt.Open(filepath.Join(s.RaftDir, "data.db"), 0600, nil)
	if err != nil {
		panic(err)
	}

	// 新建一个桶
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(DefaultKVBucketName))
		if err != nil {
			log.Fatalf("CreateBucketIfNotExists err:%s", err.Error())
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte(DefaultLeaseBucketName))
		if err != nil {
			log.Fatalf("CreateBucketIfNotExists err:%s", err.Error())
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	s.db = db

	db2, err := lsmdb.Open(lsmdb.DefaultOptions(s.RaftDir))
	if err != nil {
		panic(err)
	}
	s.db2 = db2

	// 配置Raft
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)

	newNode := !pathExists(filepath.Join(s.RaftDir, "raft.db"))

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", s.RaftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(s.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	var logStore raft.LogStore
	var stableStore raft.StableStore

	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(s.RaftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}
	logStore = boltDB
	stableStore = boltDB

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	// 单节点且是新节点 == 集群初识节点
	if enableSingle && newNode {
		s.logger.Printf("bootstrap needed")
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	} else {
		s.logger.Printf("no bootstrap needed")
	}

	return nil
}

func (s *Store) Close() {
	s.db.Close()
	s.db2.RunValueLogGC(0.7)
	s.db2.Close()
}

// WaitForLeader blocks until a leader is detected, or the timeout expires.
func (s *Store) WaitForLeader(timeout time.Duration) (string, error) {
	tck := time.NewTicker(leaderWaitDelay)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()

	for {
		select {
		case <-tck.C:
			l := s.LeaderAddr()
			if l != "" {
				return l, nil
			}
		case <-tmr.C:
			return "", fmt.Errorf("timeout expired")
		}
	}
}

// WaitForAppliedIndex blocks until a given log index has been applied,
// or the timeout expires.
func (s *Store) WaitForAppliedIndex(idx uint64, timeout time.Duration) error {
	tck := time.NewTicker(appliedWaitDelay)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()

	for {
		select {
		case <-tck.C:
			if s.raft.AppliedIndex() >= idx {
				return nil
			}
		case <-tmr.C:
			return fmt.Errorf("timeout expired")
		}
	}
}

// WaitForApplied waits for all Raft log entries to to be applied to the
// underlying database.
func (s *Store) WaitForApplied(timeout time.Duration) error {
	if timeout == 0 {
		return nil
	}
	s.logger.Printf("waiting for up to %s for application of initial logs", timeout)
	if err := s.WaitForAppliedIndex(s.raft.LastIndex(), timeout); err != nil {
		return ErrOpenTimeout
	}
	return nil
}

// RecoverStatus 节点状态机恢复后 根据状态机最新状态恢复服务现场
func (s *Store) RecoverStatus() {
	err := s.db.View(func(tx *bolt.Tx) error {
		log.Println("开始恢复数据缓存")
		bucket := tx.Bucket([]byte(DefaultKVBucketName))
		return bucket.ForEach(func(k, v []byte) error {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.m[string(k)] = v
			return nil
		})
	})

	err = s.db2.View(func(txn *lsmdb.Txn) error {
		log.Println("开始恢复租约服务")
		itr := txn.NewIterator(lsmdb.DefaultIteratorOptions)
		for itr.Rewind(); itr.Valid(); itr.Next() {
			intNum, err := strconv.Atoi(string(itr.Item().Key()))
			if err != nil {
				return err
			}
			leaseId := uint64(intNum)

			v, err := itr.Item().Value()
			if err != nil {
				return err
			}
			meta, _ := DecodeMeta(v)
			// 检测租约状态
			if meta.Status == ALIVE {
				s.wg.Wrap(func() {
					s.loopCheck(leaseId)
				})
			} else if meta.Status == CREATE {
				// TODO 处于CREATE的租约
				//      可以将这些申请了却没有使用的租约放到租约池中 之后发放新租约的时候先从租约池中取用
				// PutLeasePool(leaseId)
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}

func (s *Store) consistentRead() error {
	future := s.raft.VerifyLeader()
	if err := future.Error(); err != nil {
		return err
	}
	return nil
}

func (s *Store) Get(key string, lvl ConsistencyLevel) ([]byte, error) {
	// 如果是 Stale 读 则不管是否有leader
	if lvl != Stale {
		if s.raft.State() != raft.Leader {
			return nil, ErrNotLeader
		}
	}

	// 如果是 Consistent 读
	if lvl == Consistent {
		if err := s.consistentRead(); err != nil {
			return nil, err
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	val := make([]byte, 0)
	s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(DefaultKVBucketName))
		val = bucket.Get([]byte(key))
		return nil
	})
	return val, nil
}

func (s *Store) Set(key string, value []byte) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	c := &command{
		Op:    "set",
		Key:   key,
		Value: value,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

func (s *Store) Delete(key string) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	c := &command{
		Op:  "delete",
		Key: key,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

func (s *Store) SetMeta(key, value string) error {
	return s.Set(key, []byte(value))
}

func (s *Store) GetMeta(key string) (m string, err error) {
	b, err := s.Get(key, Stale)
	m = string(b)
	return
}

func (s *Store) DeleteMeta(key string) error {
	return s.Delete(key)
}

const (
	CREATE = iota
	ALIVE
	DEAD
)

// 创建新的租约
func (s *Store) LeaseGrant(name string, ttl int) (uint64, error) {
	if s.raft.State() != raft.Leader {
		return 0, ErrNotLeader
	}

	leaseId := s.snowflake.Generate()

	// 向状态机写入：create 新建一个Lease
	c := &command{
		Op:      "grant",
		Name:    name,
		LeaseId: leaseId,
		TTL:     ttl,
	}

	b, err := json.Marshal(c)
	if err != nil {
		return 0, err
	}
	f := s.raft.Apply(b, raftTimeout)

	s.wg.Wrap(func() {
		s.loopCheck(leaseId)
	})

	return leaseId, f.Error()
}

func (s *Store) LeaseKeepAlive(leaseId, key string, value []byte) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	intNum, err := strconv.Atoi(leaseId)
	if err != nil {
		return err
	}
	int64Num := uint64(intNum)

	if err := s.db2.View(func(txn *lsmdb.Txn) error {
		_, err := txn.Get([]byte(leaseId))
		if err != nil {
			return ErrLeaseNotFound
		}
		return nil
	}); err != nil {
		return err
	}

	c := &command{
		Op:      "keepalive",
		Key:     key,
		Value:   value,
		LeaseId: int64Num,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

func (s *Store) LeaseRevoke(leaseId string) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	intNum, err := strconv.Atoi(leaseId)
	if err != nil {
		return err
	}
	int64Num := uint64(intNum)

	if err := s.db2.View(func(txn *lsmdb.Txn) error {
		_, err := txn.Get([]byte(leaseId))
		if err != nil {
			return ErrLeaseNotFound
		}
		return nil
	}); err != nil {
		return err
	}

	c := &command{
		Op:      "revoke",
		LeaseId: int64Num,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

func (s *Store) LeaseTimeToAlive() error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	c := &command{
		Op: "timetolive",
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

func (s *Store) GetLeaseKV(prefix, key string) ([]string, error) {
	res := []string{}
	s.db2.View(func(txn *lsmdb.Txn) error {
		itr := txn.NewIterator(lsmdb.DefaultIteratorOptions)
		for itr.Rewind(); itr.Valid(); itr.Next() {
			v, err := itr.Item().Value()
			if err != nil {
				return err
			}
			meta, _ := DecodeMeta(v)
			if strings.HasPrefix(meta.Name, prefix) {
				bucket := NewBucket()
				bucket.Decode(v)
				for k := range bucket.KV {
					res = append(res, string(bucket.KV[k].V))
				}
			}
		}
		return nil
	})
	return res, nil
}

func (s *Store) loopCheck(leaseId uint64) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var start bool
	// 操作状态机 alive dead
	for {
		var count int
		select {
		case <-ticker.C:
			err := s.db2.Update(func(txn *lsmdb.Txn) error {
				item, err := txn.Get([]byte(fmt.Sprintf("%d", leaseId)))
				if err != nil {
					return ErrLeaseExpire
				}
				v, err := item.Value()
				if err != nil {
					return ErrLeaseExpire
				}
				meta, _ := DecodeMeta(v)

				if meta.Status == ALIVE {
					start = true
				} else {
					start = false
				}
				// 如果租约有租客 开始循环递减
				if start {
					c := &command{
						Op:      "loopcheck",
						LeaseId: leaseId,
					}
					b, err := json.Marshal(c)
					if err != nil {
						return err
					}
					s.raft.Apply(b, raftTimeout)
				}

				// 重新检验 Meta
				item, err = txn.Get([]byte(fmt.Sprintf("%d", leaseId)))
				if err != nil {
					return ErrLeaseExpire
				}
				v, err = item.Value()
				if err != nil {
					return ErrLeaseExpire
				}
				meta, _ = DecodeMeta(v)
				count = meta.Count

				return nil
			})
			switch err {
			case ErrLeaseExpire:
				return
			case nil:
			default:
				log.Fatalln(err)
			}

			// 租约已经生效 并且 没有存活的键值 撤销该Lease
			if count == 0 && start {
				c := &command{
					Op:      "revoke",
					LeaseId: leaseId,
				}
				b, err := json.Marshal(c)
				if err != nil {
					continue
				}
				// 向状态机写入：dead 撤销该Lease
				s.raft.Apply(b, raftTimeout)
				return
			}
		}
	}
}

// Join joins a node, identified by nodeID and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
func (s *Store) Join(nodeID, httpAddr string, addr string) error {
	s.logger.Printf("received join request for remote node %s at %s", nodeID, addr)

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				s.logger.Printf("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
				return nil
			}

			future := s.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
			}
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}

	// Set meta info
	if err := s.SetMeta(nodeID, httpAddr); err != nil {
		return err
	}

	s.logger.Printf("node %s at %s joined successfully", nodeID, addr)
	return nil
}

type fsm Store

func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Op {
	case "set":
		return f.applySet(c.Key, c.Value)
	case "delete":
		return f.applyDelete(c.Key)
	case "grant":
		return f.applyLeaseGrant(c.Name, c.LeaseId, c.TTL)
	case "loopcheck":
		return f.applyLeaseLoopCheck(c.LeaseId)
	case "keepalive":
		return f.applyLeaseKeepAlive(c.LeaseId, c.Key, c.Value)
	case "revoke":
		return f.applyLeaseRevoke(c.LeaseId)
	case "timetolive":
		return f.applyLeaseTimeToLive()
	default:
		panic(fmt.Sprintf("invalid command op: %s", c.Op))
	}
}

func (f *fsm) applySet(key string, value []byte) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(DefaultKVBucketName))
		if err := bucket.Put([]byte(key), value); err != nil {
			log.Fatalln(err)
			return err
		}
		return nil
	})
	// 更新缓存
	f.m[key] = value
	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(DefaultKVBucketName))
		if err := bucket.Delete([]byte(key)); err != nil {
			log.Fatalln(err)
			return err
		}
		return nil
	})
	// 更新缓存
	delete(f.m, key)
	return nil
}

func (f *fsm) applyLeaseGrant(name string, leaseId uint64, ttl int) interface{} {
	key := fmt.Sprintf("%d", leaseId)

	return f.db2.Update(func(txn *lsmdb.Txn) error {
		mb := EncodeMeta(&LeaseMeta{Name: name, TTL: ttl, Status: CREATE, Count: 0})
		log.Printf("发放租约 %s [%d]", name, leaseId)
		return txn.Set([]byte(key), mb)
	})
}

func (f *fsm) applyLeaseLoopCheck(leaseId uint64) interface{} {
	// Lease仍然有效 改变对应键值对的状态
	return f.db2.Update(func(txn *lsmdb.Txn) error {
		item, err := txn.Get([]byte(fmt.Sprintf("%d", leaseId)))
		if err != nil {
			return err
		}
		v, err := item.Value()
		if err != nil {
			return err
		}
		bucket := NewBucket()
		bucket.Decode(v)
		// 循环递减 TTL
		for k := range bucket.KV {
			if bucket.KV[k].T <= 0 {
				// 将该键值对删除
				delete(bucket.KV, k)
				// 标记统计值
				bucket.Meta.Count--
			} else {
				// TODO 这里发生了大量的复制
				v := bucket.KV[k]
				v.T--
				bucket.KV[k] = v
				log.Printf("[%d]:%s状态检测: TTL:%d Now:%d AliveKeys:%d",
					leaseId, k, bucket.Meta.TTL, bucket.KV[k].T, bucket.Meta.Count)
			}
		}

		// 写回数据库
		v, err = bucket.Encode()
		if err != nil {
			return err
		}
		return txn.Set([]byte(fmt.Sprintf("%d", leaseId)), v)
	})
}

// value是不带有ttl头部的值
func (f *fsm) applyLeaseKeepAlive(leaseId uint64, key string, value []byte) interface{} {
	// Lease仍然有效 改变对应键值对的状态
	return f.db2.Update(func(txn *lsmdb.Txn) error {
		item, err := txn.Get([]byte(fmt.Sprintf("%d", leaseId)))
		if err != nil {
			return err
		}
		v, err := item.Value()
		if err != nil {
			return err
		}

		bucket := NewBucket()
		bucket.Decode(v)
		// 检测这个值是否出现过 TODO 这里可能有数据竞争
		var flag bool
		for _, v := range f.leases[leaseId] {
			if v == key {
				flag = true
				break
			}
		}
		if !flag {
			bucket.Meta.Count++ // 没有出现过的新值 更新租约状态
			f.leases[leaseId] = append(f.leases[leaseId], key)
		}
		bucket.Meta.Status = ALIVE // 更新租约状态

		bucket.KV[key] = ValuePair{T: uint32(bucket.Meta.TTL), V: value}

		// 写回数据库
		v, err = bucket.Encode()
		if err != nil {
			return err
		}
		return txn.Set([]byte(fmt.Sprintf("%d", leaseId)), v)
	})
}

func (f *fsm) applyLeaseRevoke(leaseId uint64) interface{} {
	return f.db2.Update(func(txn *lsmdb.Txn) error {
		_, err := txn.Get([]byte(fmt.Sprintf("%d", leaseId)))
		if err != nil {
			return err
		}
		// TODO 还有必要标记租约失效吗
		log.Println("撤销租约", leaseId)
		return txn.Delete([]byte(fmt.Sprintf("%d", leaseId)))
	})
}

func (f *fsm) applyLeaseTimeToLive() interface{} {
	return nil
}

// Snapshot用于生成快照 snapshot, 简单的 clone map
// 在真实分布式产品中一般都要结合底层存储引擎来实现，比如 rocksdb
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the map
	o := make(map[string][]byte)
	for k, v := range f.m {
		o[k] = v
	}

	return &fsmSnapshot{store: o}, nil
}

// 回放快照
func (f *fsm) Restore(rc io.ReadCloser) error {
	o := make(map[string][]byte)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// 清空原来的 DB
	f.db.Close()
	dbFile, _ := os.OpenFile(filepath.Join(f.RaftDir, "data.db"), os.O_TRUNC, 0600)
	dbFile.Close()

	// 新建一个空的 DB
	db, err := bolt.Open(filepath.Join(f.RaftDir, "data.db"), 0600, nil)
	if err != nil {
		panic(err)
	}
	f.db = db

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(DefaultKVBucketName))
		if err != nil {
			log.Fatalf("CreateBucketIfNotExists err:%s", err.Error())
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// 用快照中的数据更新状态
	for k, v := range o {
		err = db.Batch(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(DefaultKVBucketName))
			return bucket.Put([]byte(k), v)
		})
		if err != nil {
			panic(err)
		}
	}

	// 更新缓存
	f.m = o

	return nil
}

type fsmSnapshot struct {
	store map[string][]byte
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		// Write data to sink
		if _, err := sink.Write(b); err != nil {
			return err
		}
		// Close the sink
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}
	return err
}

func (f *fsmSnapshot) Release() {}

// pathExists returns true if the given path exists.
func pathExists(p string) bool {
	if _, err := os.Lstat(p); err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}
