package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueRecord struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu sync.RWMutex

	records map[string]*ValueRecord
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.records = make(map[string]*ValueRecord, 0)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	if record, exist := kv.records[args.Key]; exist {
		reply.Value = record.value
		reply.Version = record.version
		reply.Err = rpc.OK
		return
	}
	reply.Err = rpc.ErrNoKey
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if record, exist := kv.records[args.Key]; exist {
		if record.version != args.Version {
			reply.Err = rpc.ErrVersion
			return
		}
		record.value = args.Value
		record.version++
		reply.Err = rpc.OK
		return
	}
	if args.Version == 0 {
		kv.records[args.Key] = &ValueRecord{
			value:   args.Value,
			version: 1,
		}
		reply.Err = rpc.OK
		return
	}
	reply.Err = rpc.ErrNoKey
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
