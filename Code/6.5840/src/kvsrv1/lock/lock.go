package lock

import (
	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"context"
	"fmt"
	"sync"
	"time"
)

// 定义一个密码 用于加密lock
const PW = "ZQ"

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck         kvtest.IKVClerk
	lockName   string
	clientName string
	ctx        context.Context
	// You may add code here
}

var once sync.Once

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// l is the key protected by the lock to be created.
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck: ck,
	}
	// You may add code here
	//在kv数据库中存入 【锁名】-> 【得到锁的客户端】
	//无论执行多少次 因为version一直为0 所以最终只会执行一次
	//当 lock中的值为""值时 代表当前没有人获取锁  如果lock值不为空代表当前有人得到了锁
	lockName := fmt.Sprintf("lock:%s%s", PW, l)
	nowClientName := kvtest.RandValue(8)
	lk.lockName = lockName
	lk.clientName = nowClientName
	ini := func() {
		err := lk.ck.Put(lockName, "", 0)
		if err == rpc.ErrVersion {
			kvsrv.DPrintf("重复初始化了锁")
		}
	}
	once.Do(ini)
	return lk
}

func (lk *Lock) Acquire() {

	// Your code here
	acquireLockClient, lockVersion, _ := lk.ck.Get(lk.lockName)
	nowClientName := lk.clientName
	kvsrv.DPrintf("Acquire********* Client:%s try get lock ", nowClientName)
	if acquireLockClient == "" {
		err := lk.ck.Put(lk.lockName, nowClientName, lockVersion)
		if err == rpc.OK {
			kvsrv.DPrintf("Client:%s get lock success", nowClientName)
			return
		} else {
			time.Sleep(1 * time.Second)
			kvsrv.DPrintf("Client:%s err:%v 版本有误说明当前客户端未抢到锁 锁被其他线程抢走了", nowClientName, err)
			lk.Acquire()
			return
		}
	}
	kvsrv.DPrintf("Client:%s 现在有其他客户端持有锁 将在一秒后重试", nowClientName)
	time.Sleep(1 * time.Second)
	lk.Acquire()
}

func (lk *Lock) Release() {
	// Your code here
	kvsrv.DPrintf("RELEASE:------- Client:%s try to release lock ", lk.clientName)
	acquireLockClient, lockVersion, _ := lk.ck.Get(lk.lockName)
	if acquireLockClient == "" || acquireLockClient != lk.clientName {
		return
	}
	if acquireLockClient == lk.clientName {
		err := lk.ck.Put(lk.lockName, "", lockVersion)
		if err == rpc.OK {
			kvsrv.DPrintf("RELEASE:-------  Client:%s 成功释放掉了锁", lk.clientName)
		}
	}
}
