package store

import "encoding/json"

// 这是一个性能最差的版本 之后再优化

// Bucket == metaLen + meta + map[key]struct{ttl, value}

type ValuePair struct {
	T uint32
	V []byte
}

type ValuePairs map[string]ValuePair

type Bucket struct {
	Meta LeaseMeta
	KV   ValuePairs
}

func NewBucket() *Bucket {
	return &Bucket{
		KV: make(ValuePairs),
	}
}

func (b *Bucket) AddKV(key, value []byte) {
	b.KV[string(key)] = ValuePair{T: uint32(b.Meta.TTL), V: value}
}

func (v *Bucket) DelKV(key []byte) {
	delete(v.KV, string(key))
}

func (b *Bucket) Encode() (out []byte, err error) {
	out = out[:0]
	mb := EncodeMeta(&b.Meta)
	out = append(out, mb...)
	kvb, err := json.Marshal(b.KV)
	if err != nil {
		return nil, err
	}
	out = append(out, kvb...)
	return out, nil
}

func (b *Bucket) Decode(src []byte) error {
	ml, offset := DecodeMeta(src)
	b.Meta = *ml
	return json.Unmarshal(src[offset:], &b.KV)
}
