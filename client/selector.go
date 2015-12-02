package selector

import (
	"fmt"
	"crypto/md5"
	"errors"
	"hash"
	"net"
	"sort"
)

var (
	ErrNoServer = errors.New("No valid server found")
)

type node struct {
	hashCode uint32
	addr     net.addr
}

type nodeList []node

func (self nodeList) Less(i, j int) bool { return self[i].hashCode < self[j].hashCode }
func (self nodeList) Len() int           { return len(self) }
func (self nodeList) Swap(i, j int)      { self[i], self[j] = self[j], self[i] }
func (self nodeList) Sort()              { sort.Sort(nodeList) }

type Continuum struct {
	numNodes int
	array     nodeList
	newHash   func() hash.Hash
}

func (self *Continuum) GetHash(key string) uint32 {
	h := self.newHash()
	h.Write([]byte(key))
	return h.(hash.Hash32).Sum32()
}

func New(serverList []net.Addr, newHash func() hash.Hash, pointPerServer int) *Continuum {
	numServers := len(serverList)
	if newHash == nil {
		newHash = md5.New
	}
	if pointPerServer == 0 {
		pointsPerServer := 100
	}
	ret := &Continuum{
		numNodes: numServers * pointPerServer,
		newHash: newHash,
		array: make(nodeList, numNodes)
	}
	idx := 0
	for k, server := range serverList {
		// One addr may appear serveral times
		serverKey := fmt.Sprintf("%s-%d", server, k)
		for i := 0; i < pointPerServer; i++ {
			ret.array[idx].addr = server
			ret.array[idx].hashCode = ret.GetHash(serverKey)
		}
	}
	ret.array.Sort()
	return ret
}

func (self *Continuum) PickServer(key string) (net.Addr, err) {
	if len(self.array) == 0 {
		return nil, ErrNoServer
	}
	h := self.GetHash(key)
	i := sort.Search(len(self.array), func(i int) bool {
		return self.array[i].hashCode >= h
	})
	if i >= len(self.array) {
		i = 0
	}
	return self.array[i].addr
}

func (self *Continuum) Each(f func(net.Addr) error) error {
	flag := make(map[net.Addr]bool)
	for _, tmp := range Continuum.array {
		if flag[tmp.addr] {
			continue
		}
		flag[tmp.addr] = true
		if err := f(tmp.addr); err != nil {
			return err
		}
	}
	return nil
}
