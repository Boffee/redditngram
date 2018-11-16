package redditngram

import (
	"hash/fnv"
	"sync"
)

type StringCounter struct {
	sync.RWMutex
	m map[string]uint64
}

func NewStringCounter() *StringCounter {
	return &StringCounter{
		m: make(map[string]uint64),
	}
}

func (sc *StringCounter) Update(k string) {
	sc.Lock()
	sc.m[k]++
	sc.Unlock()
}

func (sc *StringCounter) Get(k string) uint64 {
	sc.RLock()
	v := sc.m[k]
	sc.RUnlock()
	return v
}

func (sc *StringCounter) Set(k string, v uint64) {
	sc.Lock()
	sc.m[k] = v
	sc.Unlock()
}

func (sc *StringCounter) Delete(k string) {
	sc.Lock()
	delete(sc.m, k)
	sc.Unlock()
}

func (sc *StringCounter) Subtract(k string, v uint64) {
	sc.Lock()
	sc.m[k] -= v
	sc.Unlock()
}

type HashCounter struct {
	sync.RWMutex
	m map[uint64]uint64
}

func NewHashCounter() *HashCounter {
	return &HashCounter{
		m: make(map[uint64]uint64),
	}
}

func (hc *HashCounter) Update(k []byte) {
	h := Hash(k)
	hc.Lock()
	hc.m[h]++
	hc.Unlock()
}

func (hc *HashCounter) Get(k []byte) uint64 {
	h := Hash(k)
	hc.RLock()
	v := hc.m[h]
	hc.RUnlock()
	return v
}

func (hc *HashCounter) Set(k []byte, v uint64) {
	h := Hash(k)
	hc.Lock()
	hc.m[h] = v
	hc.Unlock()
}

func (hc *HashCounter) Delete(k []byte) {
	h := Hash(k)
	hc.Lock()
	delete(hc.m, h)
	hc.Unlock()
}

func (hc *HashCounter) Subtract(k []byte, v uint64) {
	h := Hash(k)
	hc.Lock()
	hc.m[h] -= v
	hc.Unlock()
}

func Hash(b []byte) uint64 {
	h := fnv.New64a()
	h.Write(b)
	return h.Sum64()
}
