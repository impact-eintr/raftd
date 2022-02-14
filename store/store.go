package store

import (
	"encoding/binary"
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
	mu sync.RWMutex
	m  map[string][]byte // 键值对缓存 用内存实现
	db *bolt.DB          // 键值对 用 impact-eintr/bolt 实现

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
		return nil
	})
	if err != nil {
		panic(err)
	}
	s.db = db

	// 建立数据缓存
	err = db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return b.ForEach(func(k, v []byte) error {
				s.mu.Lock()
				defer s.mu.Unlock()
				s.m[string(k)] = v
				return nil
			})
		})
	})
	if err != nil {
		panic(err)
	}

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

const (
	CREATE = iota
	ALIVE
	DEAD
)

type LeaseMeta struct {
	Name   string `json:"name"`
	TTL    int    `json:"ttl"`
	Status int    `json:"status"`
	Count  int    `json:"count"`
}

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
		ticker := time.NewTicker(time.Second)
		defer log.Println("退出LoopCheck")
		defer ticker.Stop()

		var start bool
		// 操作状态机 alive dead
		for {
			var count int
			select {
			case <-ticker.C:
				s.db.Update(func(tx *bolt.Tx) error {
					bucket := tx.Bucket([]byte(DefaultLeaseBucketName)).Bucket([]byte(fmt.Sprintf("%d", leaseId)))

					// 判断是否有租客
					if bucket.Stats().KeyN == 1 {
						start = false
					} else if bucket.Stats().KeyN > 1 {
						start = true
					}

					// 向状态机写入：alive 修改Lease中键值对的存活时间 TTL--
					if start {
						cur := bucket.Cursor()
						for k, v := cur.First(); k != nil; k, v = cur.Next() {
							// 跳过元数据
							if string(k) == "meta" {
								return nil
							}

							// 修改存活时间
							ttl := binary.BigEndian.Uint32(v[:4])
							ttl = ttl - 1
							// 注意由于bolt的实现机制 遍历中的value原值只可以读 不可以修改 需要先复制再Put
							b := make([]byte, len(v))
							copy(b, v)
							binary.BigEndian.PutUint32(b[:4], uint32(ttl))
							c := &command{
								Op:      "loopcheck",
								Key:     string(k),
								Value:   b,
								LeaseId: leaseId,
							}
							b, err := json.Marshal(c)
							if err != nil {
								return err
							}

							s.raft.Apply(b, raftTimeout)
						}
					}
					return nil
				})
				// 查看Lease中存活的值
				s.db.View(func(tx *bolt.Tx) error {
					bucket := tx.Bucket([]byte(DefaultLeaseBucketName)).Bucket([]byte(fmt.Sprintf("%d", leaseId)))
					if bucket == nil {
						log.Println("没有找到对应的Lease", leaseId)
						return fmt.Errorf("Lease[%d] hash been removed", leaseId)
					}

					b := bucket.Get([]byte("meta"))
					meta := new(LeaseMeta)
					err := json.Unmarshal(b, meta)
					if err != nil {
						return err
					}
					count = meta.Count
					return nil
				})

				// 租约已经生效 并且 没有存活的键值 撤销该Lease
				if count == 0 && start {
					log.Printf("%d 租约已经失效 count=%d", leaseId, count)
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
				// case exitCh:
			}
		}
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

func (s *Store) LeaseRevoke() error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	c := &command{
		Op: "revoke",
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

func (s *Store) GetLeaseKV(key string) ([]string, error) {
	res := []string{}
	s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(DefaultLeaseBucketName))
		if bucket == nil {
			return fmt.Errorf("No any KV in LeaseSystem")
		}
		return bucket.ForEach(func(k, v []byte) error {
			b := bucket.Bucket(k)
			metabytes := b.Get([]byte("meta"))
			log.Println(string(metabytes))
			meta := &LeaseMeta{}
			json.Unmarshal(metabytes, meta)
			if strings.HasPrefix(meta.Name, key) {
				b.ForEach(func(k, v []byte) error {
					if string(k) == "meta" {
						return nil
					}
					res = append(res, string(v[4:]))
					return nil
				})
			}
			return nil
		})
	})
	return res, nil
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
		return f.applyLeaseLoopCheck(c.LeaseId, c.Key, c.Value)
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

	// 创建子桶
	if err := f.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(DefaultLeaseBucketName))
		if err != nil {
			return err
		}

		bucket, err = bucket.CreateBucketIfNotExists([]byte(key))
		if err != nil {
			return err
		}

		b, err := json.Marshal(LeaseMeta{Name: name, TTL: ttl, Status: CREATE, Count: 0})
		if err != nil {
			return err
		}
		// 保存元数据
		return bucket.Put([]byte("meta"), b)
	}); err != nil {
		log.Println(err)
		return err
	}

	return nil
}

// value是带有ttl头部的值
func (f *fsm) applyLeaseLoopCheck(leaseId uint64, key string, value []byte) interface{} {
	var ttl, status, count int
	var name string
	err := f.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(DefaultLeaseBucketName)).Bucket([]byte(fmt.Sprintf("%d", leaseId)))
		if bucket == nil {
			log.Println("没有找到对应的Lease", leaseId)
			return fmt.Errorf("Lease[%d] hash been removed", leaseId)
		}

		b := bucket.Get([]byte("meta"))
		meta := new(LeaseMeta)
		err := json.Unmarshal(b, meta)
		if err != nil {
			return err
		}
		name = meta.Name
		ttl = meta.TTL
		status = meta.Status
		count = meta.Count
		return nil
	})
	if err != nil {
		return err
	}

	if status == DEAD {
		log.Println("一个已经被撤销的Lease")
		return nil
	}

	// Lease仍然有效 改变对应键值对的状态
	return f.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(DefaultLeaseBucketName)).Bucket([]byte(fmt.Sprintf("%d", leaseId)))
		if bucket == nil {
			return fmt.Errorf("Lease[%d] hash been removed", leaseId)
		}

		newttl := binary.BigEndian.Uint32(value[:4])
		log.Printf("[%d]状态检测: TTL:%d Now:%d AliveKeys:%d", leaseId, ttl, newttl, count)
		if newttl > 0 {
			b := make([]byte, len(value))
			copy(b, value)
			binary.BigEndian.PutUint32(b[:4], uint32(newttl))
			return bucket.Put([]byte(key), b)
		} else {
			// 这个键值对已经失效
			count--
			b, _ := json.Marshal(LeaseMeta{Name: name, TTL: ttl, Status: ALIVE, Count: count}) // 更新Lease状态
			bucket.Put([]byte("meta"), b)
			return bucket.Delete([]byte(fmt.Sprintf("%d", leaseId)))
		}
	})
}

// value是不带有ttl头部的值
func (f *fsm) applyLeaseKeepAlive(leaseId uint64, key string, value []byte) interface{} {
	var ttl, status, count int
	var name string
	err := f.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(DefaultLeaseBucketName)).Bucket([]byte(fmt.Sprintf("%d", leaseId)))
		if bucket == nil {
			log.Println("没有找到对应的Lease", leaseId)
			return fmt.Errorf("Lease[%d] hash been removed", leaseId)
		}

		b := bucket.Get([]byte("meta"))
		meta := new(LeaseMeta)
		err := json.Unmarshal(b, meta)
		if err != nil {
			return err
		}
		name = meta.Name
		ttl = meta.TTL
		status = meta.Status
		count = meta.Count
		return nil
	})
	if err != nil {
		return err
	}

	if status == DEAD {
		log.Println("一个已经被撤销的Lease")
		return nil
	}

	// Lease仍然有效 改变对应键值对的状态
	return f.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(DefaultLeaseBucketName)).Bucket([]byte(fmt.Sprintf("%d", leaseId)))
		if bucket == nil {
			return fmt.Errorf("Lease[%d] hash been removed", leaseId)
		}

		// 检测这个值是否出现过 TODO 这里有数据竞争
		var flag bool
		for _, v := range f.leases[leaseId] {
			if v == key {
				flag = true
				break
			}
		}
		if !flag {
			count++ // 没有出现过 是一个新值 添加一下
			f.leases[leaseId] = append(f.leases[leaseId], key)
		}

		b, _ := json.Marshal(LeaseMeta{Name: name, TTL: ttl, Status: ALIVE, Count: count}) // 更新Lease状态
		bucket.Put([]byte("meta"), b)

		// 这里是存放新值 value是原数据 没有ttl前缀
		b = make([]byte, 4+len(value))
		copy(b[4:], value)
		binary.BigEndian.PutUint32(b[:4], uint32(ttl))
		return bucket.Put([]byte(key), b)
	})
}

func (f *fsm) applyLeaseRevoke(leaseId uint64) interface{} {
	return f.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(DefaultLeaseBucketName)).Bucket([]byte(fmt.Sprintf("%d", leaseId)))
		if bucket == nil {
			return fmt.Errorf("Lease[%d] hash been removed", leaseId)
		}
		b, _ := json.Marshal(LeaseMeta{Status: DEAD}) // 更新Lease状态
		bucket.Put([]byte("meta"), b)
		return tx.Bucket([]byte(DefaultLeaseBucketName)).DeleteBucket([]byte(fmt.Sprintf("%d", leaseId)))
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
