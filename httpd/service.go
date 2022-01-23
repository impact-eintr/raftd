package httpd

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/impact-eintr/raftd/store"
)

type Store interface {
	Get(key string, lvl store.ConsistencyLevel) ([]byte, error)
	Set(key string, value []byte) error
	Delete(key string) error
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
		keyGroup.GET(":key", s.GetKeyHandler())
		keyGroup.PUT(":key", s.SetKeyHandler())
		keyGroup.DELETE(":key", s.DelKeyHandler())
	}

	r.POST("/join", func(c *gin.Context) {
		log.Println("????")
	}, s.JoinHandler())

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
