package redditngram

import (
	"hash/fnv"
	"sync"
)

type StringCounter struct {
	sync.RWMutex
	m map[string]uint32
}

func NewStringCounter() *StringCounter {
	return &StringCounter{
		m: make(map[string]uint32),
	}
}

func (sc *StringCounter) Add(k string) {
	sc.Lock()
	sc.m[k]++
	sc.Unlock()
}

func (sc *StringCounter) Update(ks []string) {
	for _, k := range ks {
		sc.Lock()
		sc.m[k]++
		sc.Unlock()
	}
}

func (sc *StringCounter) Get(k string) uint32 {
	sc.RLock()
	v := sc.m[k]
	sc.RUnlock()
	return v
}

func (sc *StringCounter) Set(k string, v uint32) {
	sc.Lock()
	sc.m[k] = v
	sc.Unlock()
}

func (sc *StringCounter) Delete(k string) {
	sc.Lock()
	delete(sc.m, k)
	sc.Unlock()
}

func (sc *StringCounter) Subtract(k string, v uint32) {
	sc.Lock()
	sc.m[k] -= v
	sc.Unlock()
}

func (sc *StringCounter) HasKey(k string) bool {
	sc.Lock()
	_, okay := sc.m[k]
	sc.Unlock()
	return okay
}

func (sc *StringCounter) GetMap() map[string]uint32 {
	return sc.m
}

type HashCounter struct {
	sync.RWMutex
	m map[uint32]uint32
}

func NewHashCounter() *HashCounter {
	return &HashCounter{
		m: make(map[uint32]uint32),
	}
}

func (hc *HashCounter) Add(k []byte) {
	h := Hash(k)
	hc.Lock()
	hc.m[h]++
	hc.Unlock()
}

func (hc *HashCounter) Update(ks [][]byte) {
	var h uint32
	for _, k := range ks {
		h = Hash(k)
		hc.Lock()
		hc.m[h]++
		hc.Unlock()
	}
}

func (hc *HashCounter) Get(k []byte) uint32 {
	h := Hash(k)
	hc.RLock()
	v := hc.m[h]
	hc.RUnlock()
	return v
}

func (hc *HashCounter) Set(k []byte, v uint32) {
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

func (hc *HashCounter) Subtract(k []byte, v uint32) {
	h := Hash(k)
	hc.Lock()
	hc.m[h] -= v
	hc.Unlock()
}

func (hc *HashCounter) HasKey(k []byte) bool {
	h := Hash(k)
	hc.Lock()
	_, okay := hc.m[h]
	hc.Unlock()
	return okay
}

func (hc *HashCounter) GetMap() map[uint32]uint32 {
	return hc.m
}

func Hash(b []byte) uint32 {
	h := fnv.New32a()
	h.Write(b)
	return h.Sum32()
}
