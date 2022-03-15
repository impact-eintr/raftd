package store

import (
	"encoding/binary"
	"log"

	"github.com/vmihailenco/msgpack/v5"
)

// Bucket == metaLen + meta + map[key]struct{ttl, value}

type LeaseMeta struct {
	Name   string `msgpack:"name"`
	TTL    int    `msgpack:"ttl"`
	Status int    `msgpack:"status"`
	Count  int    `msgpack:"count"`
}

func AssertTrue(p bool) {
	if p {
		return
	}
	panic("Assert Failed")
}

func encode(l *LeaseMeta) []byte {
	b, err := msgpack.Marshal(l)
	if err != nil {
		log.Println(err)
		return nil
	}
	return b
}

func decode(b []byte) *LeaseMeta {
	ls := new(LeaseMeta)
	err := msgpack.Unmarshal(b, ls)
	if err != nil {
		log.Println(err)
		return nil
	}
	return ls
}

func EncodeMeta(l *LeaseMeta) []byte {
	b := encode(l)
	AssertTrue(b != nil)
	buf := make([]byte, 4, 4+len(b))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(b)))
	buf = append(buf, b...)
	return buf
}

func DecodeMeta(b []byte) (*LeaseMeta, int) {
	AssertTrue(len(b) >= 4)
	ml := binary.BigEndian.Uint32(b[:4])
	AssertTrue(len(b) >= int(4+ml))
	return decode(b[4 : 4+ml]), int(4 + ml)
}
