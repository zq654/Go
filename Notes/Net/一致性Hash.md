# 什么是一致性哈希？

​	一致性哈希是一种分布式系统上用于负载均衡和用于数据分布的哈希算法。

```go
//首先思路是创建一个哈希环（有序的），然后将节点通过哈希函数映射到环上。
//然后当需要插入数据的时候会将值插入到第一个大于等于该数据哈希值的节点上。
//虚拟节点倍数将节点多划分几个 使得数据不会很集中
package main

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// ConsistentHash 一致性哈希结构体
type ConsistentHash struct {
	replicas int            // 虚拟节点倍数
	keys     []uint32       // 已排序的哈希值列表
	hashMap  map[uint32]int // 哈希值到节点索引的映射
	nodes    []int          // 节点列表
}

// NewConsistentHash 创建一个新的一致性哈希实例
func NewConsistentHash(replicas int, nodes []int) *ConsistentHash {
	ch := &ConsistentHash{
		replicas: replicas,
		hashMap:  make(map[uint32]int),
		nodes:    nodes,
	}
	// 为每个节点添加虚拟节点
	for _, node := range nodes {
		for i := 0; i < replicas; i++ {
			// 计算虚拟节点的哈希值
			hash := ch.hash(strconv.Itoa(node) + "-" + strconv.Itoa(i))
			ch.keys = append(ch.keys, hash)
			ch.hashMap[hash] = node
		}
	}
	// 对哈希值进行排序
	sort.Slice(ch.keys, func(i, j int) bool {
		return ch.keys[i] < ch.keys[j]
	})
	return ch
}

// hash 计算哈希值
func (ch *ConsistentHash) hash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// GetNode 根据键获取对应的节点
func (ch *ConsistentHash) GetNode(key string) int {
	if len(ch.keys) == 0 {
		return -1
	}
	// 计算键的哈希值
	hash := ch.hash(key)
	// 二分查找第一个大于等于哈希值的位置
	idx := sort.Search(len(ch.keys), func(i int) bool {
		return ch.keys[i] >= hash
	})
	if idx == len(ch.keys) {
		idx = 0
	}
	return ch.hashMap[ch.keys[idx]]
}

    
```

