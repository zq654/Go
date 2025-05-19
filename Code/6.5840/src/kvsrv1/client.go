package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
	"time"
)

const (
	_GET = "KVServer.Get"
	_PUT = "KVServer.Put"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	args := &rpc.GetArgs{
		Key: key,
	}
	reply := &rpc.GetReply{}
	//获取rpc消息是否被响应
	isExe := ck.clnt.Call(ck.server, _GET, args, reply)
	for !isExe {
		DPrintf("Rpc失败开始重试Get请求 %s", key)
		isExe = ck.clnt.Call(ck.server, _GET, args, reply)
		if isExe {
			DPrintf("GET SUCCESS！！！！！！ 重试成功 %s", key)
		}
		time.Sleep(10 * time.Millisecond)
	}
	err := reply.Err
	val := reply.Value
	ver := reply.Version
	return val, ver, err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	if version < 0 {
		return rpc.ErrVersion
	}
	args := &rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	reply := &rpc.PutReply{}
	isExe := ck.clnt.Call(ck.server, _PUT, args, reply)
	for !isExe {
		//若未响应则每过1秒发送一次请求
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		select {
		case <-ticker.C:
			DPrintf("Rpc失败开始重试Put请求 KEY:%s  VALUE:%s", key, value)
			isExe = ck.clnt.Call(ck.server, _PUT, args, reply)
			if isExe && reply.Err == rpc.ErrVersion {
				DPrintf("Put SUCCESS！！！！！！ 重试成功 KEY:%s  VALUE:%s", key, value)
				reply.Err = rpc.ErrMaybe
			}
		}
	}
	return reply.Err
}
