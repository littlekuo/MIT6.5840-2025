package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck            kvtest.IKVClerk
	lockKey       string
	id            string
	lockedVersion rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, lockKey: l, id: kvtest.RandValue(8)}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.lockKey)
		var putErr rpc.Err
		if err == rpc.ErrNoKey {
			putErr = lk.ck.Put(lk.lockKey, lk.id, 0)
		} else {
			if value == lk.id {
				// locked by me
				lk.lockedVersion = version
				break
			}
			if value != "" {
				// already locked by others
				time.Sleep(10 * time.Millisecond)
				continue
			}
			putErr = lk.ck.Put(lk.lockKey, lk.id, version)
		}
		if putErr == rpc.OK {
			lk.lockedVersion = version + 1
			break
		}
		if putErr == rpc.ErrVersion {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (lk *Lock) Release() {
	lk.ck.Put(lk.lockKey, "", lk.lockedVersion)
}
