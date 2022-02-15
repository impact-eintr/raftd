package httpd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/impact-eintr/raftd/store"
)

type Store interface {
	Get(key string, lvl store.ConsistencyLevel) ([]byte, error)
	Set(key string, value []byte) error
	Delete(key string) error
	LeaseGrant(name string, ttl int) (uint64, error)
	LeaseKeepAlive(leaseId, key string, value []byte) error
	LeaseRevoke() error
	LeaseTimeToAlive() error
	GetLeaseKV(prefix, key string) ([]string, error)
	Join(nodeID string, httpAddress string, addr string) error
	LeaderAPIAddr() string
	SetMeta(key, value string) error
}

type Service struct {
	addr string
	ln   net.Listener

	store Store
}

// FormRedirect returns the value for the "Location" header for a 301 response.
func (s *Service) FormRedirect(r *http.Request, host string) string {
	protocol := "http"
	rq := r.URL.RawQuery
	if rq != "" {
		rq = fmt.Sprintf("?%s", rq)
	}
	return fmt.Sprintf("%s://%s%s%s", protocol, host, r.URL.Path, rq)
}

func New(addr string, store Store) *Service {
	return &Service{
		addr:  addr,
		store: store,
	}
}

func (s *Service) Start() error {
	server := http.Server{
		Handler: s.newRouter(),
	}
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln

	go func() {
		err := server.Serve(s.ln)
		if err != nil {
			log.Fatalln(err)
		}
	}()

	return nil
}

func (s *Service) Close() {
	s.ln.Close()
	return
}

// Addr returns the address on which the Service is listening
func (s *Service) Addr() net.Addr {
	return s.ln.Addr()
}

func (s *Service) newRouter() (r *gin.Engine) {
	r = gin.Default()

	keyGroup := r.Group("/key")
	{
		keyGroup.GET("/:key", s.GetKeyHandler())
		keyGroup.PUT("/:key", s.SetKeyHandler())
		keyGroup.DELETE("/:key", s.DelKeyHandler())
	}

	leaseGroup := r.Group("/lease")
	{
		leaseGroup.POST("/grant", s.LeaseGrantHandler())
		leaseGroup.POST("/keepalive/:id", s.LeaseKeepAliveHandler())
		leaseGroup.POST("/revoke/:id", s.LeaseRevokeHandler())
		leaseGroup.POST("/timetolive/:id", s.LeaseTimeToAliveHandler())
		leaseGroup.POST("/kv/:key", s.GetKeyByLeaseHandler())
	}

	r.POST("/join", s.JoinHandler())

	return r
}

func level(req *http.Request) (store.ConsistencyLevel, error) {
	q := req.URL.Query()
	lvl := strings.TrimSpace(q.Get("level"))

	switch strings.ToLower(lvl) {
	case "default":
		return store.Default, nil
	case "stale":
		return store.Stale, nil
	case "consistent":
		return store.Consistent, nil
	default:
		return store.Default, nil
	}
}

/* Key-Value存储 */

// 默认返回 Default 一致性级别的Key
func (s *Service) GetKeyHandler() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		k := ctx.Param("key")
		if k == "" {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}

		lvl, err := level(ctx.Request)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		v, err := s.store.Get(k, lvl)
		if err != nil {
			if err == store.ErrNotLeader {
				leader := s.store.LeaderAPIAddr()
				if leader == "" {
					ctx.JSON(http.StatusServiceUnavailable, err)
					return
				}
				redirect := s.FormRedirect(ctx.Request, leader)
				ctx.Redirect(http.StatusTemporaryRedirect, redirect)
				return
			}
			ctx.JSON(http.StatusInternalServerError, err)
			return
		}
		io.Copy(ctx.Writer, bytes.NewReader(v))
	}
}

func (s *Service) SetKeyHandler() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		k := ctx.Param("key")
		if k == "" {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		b, err := ioutil.ReadAll(ctx.Request.Body)
		if err != nil {
			ctx.JSON(http.StatusInternalServerError, err)
		}

		if err := s.store.Set(k, b); err != nil {
			if err == store.ErrNotLeader {
				leader := s.store.LeaderAPIAddr()
				if leader == "" {
					ctx.JSON(http.StatusServiceUnavailable, err)
					return
				}
				redirect := s.FormRedirect(ctx.Request, leader)
				ctx.Redirect(http.StatusTemporaryRedirect, redirect)
				return
			}

			ctx.JSON(http.StatusInternalServerError, err)
			return
		}
	}
}

func (s *Service) DelKeyHandler() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		k := ctx.Param("key")
		if k == "" {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}

		if err := s.store.Delete(k); err != nil {
			if err == store.ErrNotLeader {
				leader := s.store.LeaderAPIAddr()
				if leader == "" {
					ctx.JSON(http.StatusServiceUnavailable, err)
					return
				}
				redirect := s.FormRedirect(ctx.Request, leader)
				ctx.Redirect(http.StatusTemporaryRedirect, redirect)
				return
			}
			ctx.JSON(http.StatusInternalServerError, err)
			return
		}
		s.store.Delete(k) // TODO 为什么要执行两次
	}
}

func (s *Service) JoinHandler() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		m := make(map[string]string)
		if err := ctx.ShouldBindJSON(&m); err != nil {
			ctx.JSON(http.StatusBadRequest, err)
			return
		}
		if len(m) != 3 {
			ctx.JSON(http.StatusBadRequest, errors.New("invalid number of config"))
			return
		}

		httpAddr, ok := m["httpAddr"]
		if !ok {
			ctx.JSON(http.StatusBadRequest, errors.New("invalid httpAddr"))
			return
		}

		raftAddr, ok := m["raftAddr"]
		if !ok {
			ctx.JSON(http.StatusBadRequest, errors.New("invalid raftAddr"))
			return
		}

		nodeID, ok := m["id"]
		if !ok {
			ctx.JSON(http.StatusBadRequest, errors.New("invalid nodeID"))
			return
		}

		if err := s.store.Join(nodeID, httpAddr, raftAddr); err != nil {
			if err == store.ErrNotLeader {
				leader := s.store.LeaderAPIAddr()
				if leader == "" {
					ctx.JSON(http.StatusServiceUnavailable, err)
					return
				}
				redirect := s.FormRedirect(ctx.Request, leader)
				ctx.Redirect(http.StatusTemporaryRedirect, redirect)
				return
			}
			ctx.JSON(http.StatusInternalServerError, err)
			return
		}
	}
}

/* 租约机制 NOTICE 是独立的*/

// LeaseGrant 创建一个租约 现在只支持前缀匹配索引
func (s *Service) LeaseGrantHandler() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		q := ctx.Request.URL.Query()
		ttl := strings.TrimSpace(q.Get("ttl"))
		if ttl == "" {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		ttlnum, err := strconv.Atoi(ttl)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}

		name := strings.TrimSpace(q.Get("name"))
		if name == "" {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}

		v, err := s.store.LeaseGrant(name, ttlnum) // TODO 这里还可以承载更多的数据
		if err != nil {
			if err == store.ErrNotLeader {
				leader := s.store.LeaderAPIAddr()
				if leader == "" {
					ctx.JSON(http.StatusServiceUnavailable, err.Error())
					return
				}
				redirect := s.FormRedirect(ctx.Request, leader)
				ctx.Redirect(http.StatusTemporaryRedirect, redirect)
				return
			}
			ctx.JSON(http.StatusInternalServerError, err.Error())
			return
		}
		ctx.JSON(http.StatusOK, v)
	}
}

// LeaseKeepAlive 用于维持租约
func (s *Service) LeaseKeepAliveHandler() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		leaseId := ctx.Param("id")
		if leaseId == "" {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}

		// TODO 这里还应该可以使用 LeaseName 索引(咕咕咕

		// TODO 这里的数据来自客户端的请求
		// -XPOST -d 'key=/esq/node-1' -d 'data={"body":"this is a job","topic":"ketang","delay":0,"route_key":"homework"}'
		key := ctx.Request.FormValue("key")
		data := ctx.Request.FormValue("data")
		if len(key) == 0 || len(data) == 0 {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		//log.Println("维持租约的数据", key, data)

		err := s.store.LeaseKeepAlive(leaseId, key, []byte(data))
		if err != nil {
			if err == store.ErrNotLeader {
				leader := s.store.LeaderAPIAddr()
				if leader == "" {
					ctx.JSON(http.StatusServiceUnavailable, err.Error())
					return
				}
				redirect := s.FormRedirect(ctx.Request, leader)
				ctx.Redirect(http.StatusTemporaryRedirect, redirect)
				return
			} else if err == store.ErrLeaseNotFound {
				ctx.JSON(http.StatusNotFound, err.Error())
				return
			}
			ctx.JSON(http.StatusInternalServerError, err.Error())
			return
		}
		ctx.JSON(http.StatusOK, nil)
	}
}

// LeaseRevoke 撤销一个租约
func (s *Service) LeaseRevokeHandler() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		leaseId := ctx.Param("id")
		if leaseId == "" {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
	}
}

// LeaseTimeToAlive 获取租约信息
func (s *Service) LeaseTimeToAliveHandler() gin.HandlerFunc {
	return func(c *gin.Context) {}
}

// 通过前缀和键名 索引租约 现在只支持前缀匹配索引
func (s *Service) GetKeyByLeaseHandler() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		key := ctx.Param("key")
		if key == "" {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}

		// -XPOST -d 'prefix=/esq/node'
		prefix := ctx.Request.FormValue("prefix")
		if len(prefix) == 0 {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		//log.Println("通过前缀匹配 获取租约中的KV", prefix, key)

		v, err := s.store.GetLeaseKV(prefix, key)
		if err != nil {
			if err == store.ErrNotLeader {
				leader := s.store.LeaderAPIAddr()
				if leader == "" {
					ctx.JSON(http.StatusServiceUnavailable, err)
					return
				}
				redirect := s.FormRedirect(ctx.Request, leader)
				ctx.Redirect(http.StatusTemporaryRedirect, redirect)
				return
			}
			ctx.JSON(http.StatusInternalServerError, err)
			return
		}
		res := []map[string]interface{}{}
		for k := range v {
			m := make(map[string]interface{})
			json.Unmarshal([]byte(v[k]), &m)
			res = append(res, m)
		}
		ctx.JSON(http.StatusOK, res)
	}
}
