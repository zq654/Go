package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	PW          = "ZQ"
	Debug       = false
	Version_One = 1
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	//todo 现暂时使用锁来控制线程 后续需优化为带租约的锁 当server挂掉时也能释放掉锁
	mu sync.RWMutex
	// Your definitions here.
	data   map[MyKey]*MyValue
	cancel context.CancelFunc
}
type MyKey string

type MyValue struct {
	Version rpc.Tversion
	Value   string
}

func MakeKVServer() *KVServer {
	// Your code here.
	kv := &KVServer{
		data: make(map[MyKey]*MyValue),
		mu:   sync.RWMutex{},
	}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	key := MyKey(args.Key)
	// 读取数据时记得上读锁
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	value, ok := kv.data[key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Version = value.Version
	reply.Value = value.Value
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {

	// Your code here.
	version := args.Version
	key := MyKey(args.Key)
	value := args.Value
	kv.mu.Lock()
	defer kv.mu.Unlock()
	oldValue, ok := kv.data[key]

	defer func() {
		if reply.Err == rpc.OK && strings.Contains(args.Key, fmt.Sprintf("lock:%s", PW)) && value != "" {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			kv.cancel = cancel
			go func() {
				select {
				case <-ctx.Done():
					if ctx.Err() == context.DeadlineExceeded {
						kv.data[MyKey(args.Key)].Value = ""
						DPrintf("lock:%s 锁过了超时时间1秒自动释放掉了", args.Key)
					} else if ctx.Err() == context.Canceled {
						DPrintf("lock:%s 锁被手动释放掉了", args.Key)
					}
				}
			}()
		} else if reply.Err == rpc.OK && strings.Contains(args.Key, fmt.Sprintf("lock:%s", PW)) && value == "" {
			if kv.cancel != nil {
				kv.cancel()
			}
		}
	}()
	//若不ok 代表这是第一次Put操作
	if !ok {
		//第一次Put 需要判断version是否为0 若不为0则return code 防止重传时重复put
		if version != rpc.Tversion(0) {
			reply.Err = rpc.ErrNoKey
			return
		}
		//若第一次Put version为0 则将值放在map中 记得Version设为1
		myValue := &MyValue{
			Version: Version_One,
			Value:   value,
		}

		kv.data[key] = myValue
		reply.Err = rpc.OK
		return
	}
	//若并非第一次Put操作
	if oldValue.Version != version {
		reply.Err = rpc.ErrVersion
		return
	}
	oldValue.Value = value
	oldValue.Version = version + 1
	reply.Err = rpc.OK

}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
