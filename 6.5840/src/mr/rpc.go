package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type ErrorCode uint32

const (
	ErrCode_OK ErrorCode = iota
	ErrCode_ALL_DONE
	ErrCode_RETRY
	ErrCode_BAD_REQUEST
	ErrCode_WORKER_NOT_FOUND
	ErrCode_WORKER_NOT_MATCH
	ErrCode_INVALID_STAGE
	ErrorCode_TASK_NOT_FOUND
)

const (
	MapStage uint32 = iota
	ReduceStage
	DoneStage
)

const (
	InvalidWorkerID int32 = -1
)

type TaskInfo struct {
	TaskStage uint32
	TaskNum   int
	Files     []string
}

type RegisterWorkerRequest struct {
}

type RegisterWorkerResponse struct {
	WorkerID  int32
	ReduceNum int
}

type GetTaskRequest struct {
	WokerID int32
}

type GetTaskResponse struct {
	ErrCode  ErrorCode
	TaskInfo *TaskInfo
}

type NotifyTaskDoneRequest struct {
	WokerID           int32
	TaskNum           int
	TaskStage         uint32
	IntermediateFiles []string //only invalid in MapStage
}

type NotifyTaskDoneResponse struct {
	ErrCode ErrorCode
}

func GetStageString(stage uint32) string {
	switch stage {
	case MapStage:
		return "Map"
	case ReduceStage:
		return "Reduce"
	case DoneStage:
		return "Done"
	default:
		return "Invalid"
	}
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
