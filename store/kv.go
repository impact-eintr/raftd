package store

import (
	"encoding/binary"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

const (
	IndexSize      = 5 * 4              // ko+ks+vo+vs+ttl
	BlockIndexSize = 4 + 1024*IndexSize // currOffset + 1024*(ko+ks+vo+vs+ttl)
)

// [co][ko ks vo vs ttl ...][nbo][k v k v ...] [co][ko ks vo vs ttl ...][nbo][k v k v ...]...

// 会返回填充过的 kvb 小心使用
func appendKV(kvb, k, v []byte, ttl uint32) []byte {
	if len(k) == 0 || len(v) == 0 {
		return kvb
	}

	// 初始化
	if len(kvb) == 0 {
		kvb = make([]byte, BlockIndexSize+4)
		binary.BigEndian.PutUint32(kvb[:], 4) // 读写指针初始化
	}

	buf := kvb
	for {
		curOffset := binary.BigEndian.Uint32(buf[:4])
		if curOffset < BlockIndexSize {
			binary.BigEndian.PutUint32(buf[:4], curOffset+IndexSize)     // 标记当前数据指针后移
			buf = buf[curOffset:]                                        // 定位到当前读写指针处
			binary.BigEndian.PutUint32(buf[:], uint32(len(kvb)))         // keyOffset
			binary.BigEndian.PutUint32(buf[4:], uint32(len(k)))          // keyLen
			binary.BigEndian.PutUint32(buf[8:], uint32(len(kvb)+len(k))) // valueOffset
			binary.BigEndian.PutUint32(buf[12:], uint32(len(v)))         // valueLen
			binary.BigEndian.PutUint32(buf[16:], ttl)

			// 满了 标注 NextBlockOffset
			if curOffset == BlockIndexSize-IndexSize {
				// 往后一个 indexSize 就是 nextblockOffset
				buf = buf[IndexSize:]
				binary.BigEndian.PutUint32(buf[:], uint32(len(kvb)+len(k)+len(v)))
				newBlockIndex := make([]byte, 4+BlockIndexSize)
				binary.BigEndian.PutUint32(newBlockIndex[:], 4) // 读写指针初始化
				kvb = append(kvb, k...)
				kvb = append(kvb, v...)
				kvb = append(kvb, newBlockIndex...)
			} else {
				kvb = append(kvb, k...)
				kvb = append(kvb, v...)
			}

			break
		} else {
			// 准备跳到下一个数据块
			buf = buf[BlockIndexSize:]
			nextblockOffset := binary.BigEndian.Uint32(buf[:4]) // 读取下一个数据块索引开始的位置

			buf = kvb[nextblockOffset:]
		}
	}
	return kvb
}

// TODO 性能现在太差了 之后需要优化 用一下 XMM ?
func addKV(kvb, k, v []byte) (out []byte) {
	if len(k) == 0 || len(v) == 0 {
		return kvb
	}
	var m = make(map[string][]byte)
	var err error

	if len(kvb) > 0 {
		err = msgpack.Unmarshal(kvb, &m)
		if err != nil {
			panic(err)
		}
	}

	m[string(k)] = v

	out, err = msgpack.Marshal(m)
	if err != nil {
		panic(err)
	}

	return out
}

func delKV(kvb, k []byte) (out []byte) {
	var m = make(map[string][]byte)
	err := msgpack.Unmarshal(kvb, &m)
	if err != nil {
		panic(err)
	}

	delete(m, string(k))

	out, err = msgpack.Marshal(m)
	if err != nil {
		panic(err)
	}

	return out
}

// 遍历 key-value pair
func traverseKV(kvb []byte, cb func([]byte, []byte) error) {
	var m = make(map[string][]byte)
	err := msgpack.Unmarshal(kvb, &m)
	if err != nil {
		panic(err)
	}

	for k := range m {
		cb([]byte(k), m[k])
	}
}

func rangeKV(kvb []byte, cb func([]byte, []byte) error) {
	if len(kvb) == 0 {
		return
	}

	defer func() {
		if err := recover(); err != nil {
			fmt.Println("出错了", err)
		}
	}()

	decode := func(buf []byte) ([]byte, []byte) {
		// 内层小循环是段内遍历
		ko := binary.BigEndian.Uint32(buf[:])   // keyOffset
		kl := binary.BigEndian.Uint32(buf[4:])  // keyLen
		vo := binary.BigEndian.Uint32(buf[8:])  // valueOffset
		vl := binary.BigEndian.Uint32(buf[12:]) // valueLen
		//ttl := binary.BigEndian.Uint32(buf[16:])
		return kvb[ko : ko+kl], kvb[vo : vo+vl]
	}

	buf := kvb
	for {
		curOffset := binary.BigEndian.Uint32(buf[:4])
		// 外层大循环是逐段跳转
		if curOffset < BlockIndexSize {
			// 未写满的一块数据
			buf = buf[4:] // 定位到当前读写指针处
			for i := 4; i < int(curOffset); i += IndexSize {
				key, value := decode(buf)

				buf = buf[IndexSize:] // 移动指针
				cb(key, value)
			}
			break
		} else if curOffset == BlockIndexSize {
			// 写满的一块数据
			buf = buf[4:] // 定位到当前读写指针处
			for i := 4; i < BlockIndexSize; i += IndexSize {
				key, value := decode(buf)

				if i != BlockIndexSize-IndexSize {
					buf = buf[IndexSize:] // 移动指针
				} else {
					buf = buf[IndexSize:] // 移动指针 到下一个 block
					nbo := binary.BigEndian.Uint32(buf[:4])
					buf = kvb[nbo:]
				}
				cb(key, value)
			}
		}
	}
}
