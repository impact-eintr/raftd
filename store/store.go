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
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/impact-eintr/bolt"
)

const (
	DefaultBucketName = "impact-eintr"
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
}

type ConsistencyLevel int

const (
	Default ConsistencyLevel = iota
	Stale
	Consistent
)

type Store struct {
	RaftDir  string
	RaftBind string

	// 保护m的锁
	mu sync.RWMutex
	m  map[string][]byte // 键值对缓存 用内存实现
	db *bolt.DB          // 键值对 用 impact-eintr/bolt 实现

	raft *raft.Raft // 一致性机制

	logger *log.Logger
}

func New() *Store {
	return &Store{
		m:      make(map[string][]byte),
		logger: log.New(os.Stderr, "[store] ", log.LstdFlags),
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
		_, err := tx.CreateBucketIfNotExists([]byte(DefaultBucketName))
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
		bucket := tx.Bucket([]byte(DefaultBucketName))
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
	default:
		panic(fmt.Sprintf("invalid command op: %s", c.Op))
	}
}

func (f *fsm) applySet(key string, value []byte) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(DefaultBucketName))
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
		bucket := tx.Bucket([]byte(DefaultBucketName))
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
		_, err := tx.CreateBucketIfNotExists([]byte(DefaultBucketName))
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
			bucket := tx.Bucket([]byte(DefaultBucketName))
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
