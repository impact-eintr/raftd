package store

import (
	"encoding/binary"
	"encoding/json"
)

type LeaseMeta struct {
	Name   string `json:"name"`
	TTL    int    `json:"ttl"`
	Status int    `json:"status"`
	Count  int    `json:"count"`
}

func AssertTrue(p bool) {
	if p {
		return
	}
	panic("Assert Failed")
}

func encode(l *LeaseMeta) []byte {
	b, err := json.Marshal(l)
	if err != nil {
		return nil
	}
	return b
}

func decode(b []byte) *LeaseMeta {
	ls := new(LeaseMeta)
	err := json.Unmarshal(b, ls)
	if err != nil {
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
	return decode(b[4:ml]), int(4 + ml)
}
