package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultTaskTimeout = time.Second * 10
)

type TaskState struct {
	WorkerID  int32
	IsDone    bool
	StartTime time.Time
	EndTime   time.Time
}

type Coordinator struct {
	inputFiles []string
	nReduce    int

	tasksLock       sync.RWMutex
	unassignedTasks []int
	ongoingTasks    map[int]*TaskState
	completedTasks  map[int]*TaskState
	intermediateRet map[int][]string // mapTaskNum -> files (order by reduceNum)
	reduceTasks     [][]string       // reduceTaskNum -> files

	idAllocator       uint32
	curStage          uint32
	registerLock      sync.RWMutex
	registeredWorkers map[int32]struct{}
}

func (c *Coordinator) checkOngoingTaskTimeoutLocked() bool {
	timeoutTasks := make([]int, 0)
	for taskNum, taskState := range c.ongoingTasks {
		if taskState.IsDone {
			log.Printf("task %d is done, is not as expected\n", taskNum)
			continue
		}

		if time.Since(taskState.StartTime) > DefaultTaskTimeout {
			log.Printf("task [%d] timeout, worker id [%d], stage [%s]\n",
				taskNum, taskState.WorkerID, GetStageString(c.curStage))
			timeoutTasks = append(timeoutTasks, taskNum)
		}
	}
	if len(timeoutTasks) <= 0 {
		return false
	}
	for _, taskNum := range timeoutTasks {
		c.unassignedTasks = append(c.unassignedTasks, taskNum)
		delete(c.ongoingTasks, taskNum)
	}
	return true
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RegisterWorker(req *RegisterWorkerRequest, response *RegisterWorkerResponse) error {
	response.WorkerID = int32(atomic.AddUint32(&c.idAllocator, 1))
	response.ReduceNum = c.nReduce
	c.registerLock.Lock()
	c.registeredWorkers[response.WorkerID] = struct{}{}
	c.registerLock.Unlock()
	return nil
}

func (c *Coordinator) GetTask(req *GetTaskRequest, response *GetTaskResponse) error {
	c.registerLock.RLock()
	if _, exist := c.registeredWorkers[req.WokerID]; !exist {
		c.registerLock.RUnlock()
		response.ErrCode = ErrCode_WORKER_NOT_FOUND
		return nil
	}
	c.registerLock.RUnlock()

	c.tasksLock.Lock()
	defer c.tasksLock.Unlock()

	if c.curStage == DoneStage {
		response.ErrCode = ErrCode_ALL_DONE
		return nil
	}

	if len(c.unassignedTasks) == 0 {
		if !c.checkOngoingTaskTimeoutLocked() {
			// means no task can be assigned, so we can return directly
			response.ErrCode = ErrCode_RETRY
			return nil
		}
	}

	taskNum := c.unassignedTasks[0]
	c.unassignedTasks = c.unassignedTasks[1:]
	c.ongoingTasks[taskNum] = &TaskState{
		WorkerID:  req.WokerID,
		IsDone:    false,
		StartTime: time.Now(),
	}

	response.ErrCode = ErrCode_OK

	switch c.curStage {
	case MapStage:
		response.TaskInfo = &TaskInfo{
			Files:     []string{c.inputFiles[taskNum]},
			TaskStage: c.curStage,
			TaskNum:   taskNum,
		}
	case ReduceStage:
		response.TaskInfo = &TaskInfo{
			Files:     c.reduceTasks[taskNum],
			TaskStage: c.curStage,
			TaskNum:   taskNum,
		}

	default:
		log.Fatal("invalid stage")
	}
	return nil
}

func (c *Coordinator) NotifyTaskDone(req *NotifyTaskDoneRequest, response *NotifyTaskDoneResponse) error {
	c.registerLock.RLock()
	if _, exist := c.registeredWorkers[req.WokerID]; !exist {
		c.registerLock.RUnlock()
		response.ErrCode = ErrCode_WORKER_NOT_FOUND
		return nil
	}
	c.registerLock.RUnlock()

	c.tasksLock.Lock()
	defer c.tasksLock.Unlock()

	if c.curStage == DoneStage {
		response.ErrCode = ErrCode_ALL_DONE
		return nil
	}

	if req.TaskStage != c.curStage {
		response.ErrCode = ErrCode_INVALID_STAGE
		return nil
	}

	if _, exist := c.completedTasks[req.TaskNum]; exist {
		log.Printf("task %d has been done, stage:%d", req.TaskNum, c.curStage)
		response.ErrCode = ErrCode_OK
		return nil
	}

	taskState, exist := c.ongoingTasks[req.TaskNum]
	if !exist {
		response.ErrCode = ErrorCode_TASK_NOT_FOUND
		return nil
	}
	if taskState.WorkerID != req.WokerID {
		response.ErrCode = ErrCode_WORKER_NOT_MATCH
		return nil
	}

	taskState.EndTime = time.Now()
	taskState.IsDone = true
	c.completedTasks[req.TaskNum] = taskState
	delete(c.ongoingTasks, req.TaskNum)

	switch c.curStage {
	case MapStage:
		c.intermediateRet[req.TaskNum] = req.IntermediateFiles
		if len(c.completedTasks) == len(c.inputFiles) {
			atomic.StoreUint32(&c.curStage, uint32(ReduceStage))
			c.unassignedTasks = make([]int, 0)
			c.ongoingTasks = make(map[int]*TaskState)
			c.completedTasks = make(map[int]*TaskState)
			for idx := 0; idx < c.nReduce; idx++ {
				c.unassignedTasks = append(c.unassignedTasks, idx)
			}
			for _, mapFiles := range c.intermediateRet {
				for idx, file := range mapFiles {
					c.reduceTasks[idx] = append(c.reduceTasks[idx], file)
				}
			}
		}
	case ReduceStage:
		if len(c.completedTasks) == c.nReduce {
			response.ErrCode = ErrCode_ALL_DONE
			atomic.StoreUint32(&c.curStage, uint32(DoneStage))
		}
	default:
		log.Fatal("invalid stage")
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return atomic.LoadUint32(&c.curStage) == uint32(DoneStage)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.inputFiles = files
	c.nReduce = nReduce
	for idx, _ := range c.inputFiles {
		c.unassignedTasks = append(c.unassignedTasks, idx)
	}
	c.ongoingTasks = make(map[int]*TaskState)
	c.completedTasks = make(map[int]*TaskState)
	c.intermediateRet = make(map[int][]string)
	c.reduceTasks = make([][]string, nReduce)

	c.idAllocator = 0
	c.curStage = MapStage
	c.registeredWorkers = make(map[int32]struct{})

	c.server()
	return &c
}
