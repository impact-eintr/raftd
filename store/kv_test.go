package store

import (
	"fmt"
	"testing"
)

//func TestTraverseKV(t *testing.T) {
//	var kvb []byte
//	for i := 0; i < 1000; i++ {
//		kvb = addKV(kvb, []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
//	}
//	//traverseKV(kvb, func(k []byte, v []byte) error {
//	//	log.Println(string(k), string(v))
//	//	return nil
//	//})
//}

func TestAppendKV(t *testing.T) {
	var kvb []byte
	for i := 0; i < 200000; i++ {
		kvb = appendKV(kvb, []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)), 1)
	}

	rangeKV(kvb, func(k []byte, v []byte) error {
		//log.Println(string(k), string(v))
		return nil
	})
}
