package store

import (
	"fmt"
	"log"
	"testing"
)

func TestAppendKV(t *testing.T) {
	var kvb []byte
	for i := 0; i < 20000; i++ {
		kvb = appendKV(kvb, []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)), 1)
	}

	rangeKV(kvb, func(k []byte, v []byte, ttl uint32) error {
		log.Println(string(k), string(v))
		return nil
	})
}
