package mr

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type HeapItem struct {
	kv       *KeyValue // 当前元素值
	sliceIdx int       // 来自哪个子数组
	itemIdx  int       // 在子数组中的位置
}

type MinHeap []HeapItem

func (h MinHeap) Len() int           { return len(h) }
func (h MinHeap) Less(i, j int) bool { return h[i].kv.Key < h[j].kv.Key }
func (h MinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(HeapItem))
}

func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func kWayMerge(slices [][]KeyValue) []KeyValue {
	h := &MinHeap{}
	heap.Init(h)

	for i, slice := range slices {
		if len(slice) > 0 {
			heap.Push(h, HeapItem{
				kv:       &slice[0],
				sliceIdx: i,
				itemIdx:  0,
			})
		}
	}

	result := make([]KeyValue, 0)

	for h.Len() > 0 {
		current := heap.Pop(h).(HeapItem)
		result = append(result, *current.kv)

		if current.itemIdx+1 < len(slices[current.sliceIdx]) {
			nextItem := HeapItem{
				kv:       &slices[current.sliceIdx][current.itemIdx+1],
				sliceIdx: current.sliceIdx,
				itemIdx:  current.itemIdx + 1,
			}
			heap.Push(h, nextItem)
		}
	}

	return result
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerID := InvalidWorkerID
	nReduce := 0

	for {
		if workerID == InvalidWorkerID {
			registerResp, rErr := RegisterWorker()
			if rErr != nil {
				log.Fatalf("register worker failed: %v", rErr)
			}
			workerID = registerResp.WorkerID
			nReduce = registerResp.ReduceNum
		}
		taskResp, err := GetTask(workerID)
		if err != nil {
			log.Fatalf("get task failed: %v", err)
		}
		if taskResp.ErrCode == ErrCode_WORKER_NOT_FOUND {
			workerID = InvalidWorkerID
			continue
		}
		if taskResp.ErrCode == ErrCode_ALL_DONE {
			return
		}
		if taskResp.ErrCode == ErrCode_RETRY {
			time.Sleep(time.Second)
			continue
		}

		var notifyResp *NotifyTaskDoneResponse
		var handleErr, notifyErr error
		taskInfo := taskResp.TaskInfo
		switch taskInfo.TaskStage {
		case MapStage:
			content, rErr := ReadFile(taskInfo.Files[0])
			if rErr != nil {
				continue
			}
			kvMap := mapf(taskInfo.Files[0], string(content))
			intermediateKvs := make([][]KeyValue, nReduce)
			intermediateFiles := make([]string, nReduce)
			for _, kv := range kvMap {
				reduceTaskNum := ihash(kv.Key) % nReduce
				intermediateKvs[reduceTaskNum] = append(intermediateKvs[reduceTaskNum], kv)
			}
			for i, kvs := range intermediateKvs {
				sort.Sort(ByKey(kvs))
				fileName := fmt.Sprintf("mr-inter-%d-%d-%d", workerID, taskInfo.TaskNum, i)
				tempFileName := "tmp-" + fileName
				ret, mErr := json.Marshal(kvs)
				if mErr != nil {
					log.Printf("marshal kvs failed: %v\n", mErr)
					handleErr = mErr
					break
				}
				if err := os.WriteFile(tempFileName, ret, 0666); err != nil {
					log.Printf("write intermediate file failed: %v", err)
					handleErr = err
					break
				}
				if err := os.Rename(tempFileName, fileName); err != nil {
					log.Printf("rename intermediate file failed: %v", err)
					handleErr = err
					break
				}
				intermediateFiles[i] = fileName
			}
			if handleErr != nil {
				continue
			}
			notifyResp, notifyErr = NotifyTaskDone(workerID, taskInfo.TaskNum, taskInfo.TaskStage, intermediateFiles)
		case ReduceStage:
			multiKvs := make([][]KeyValue, 0)
			for _, file := range taskInfo.Files {
				content, rErr := ReadFile(file)
				if rErr != nil {
					handleErr = err
					break
				}
				kvs := []KeyValue{}
				uErr := json.Unmarshal(content, &kvs)
				if uErr != nil {
					log.Printf("unmarshal intermediate file failed: %v", uErr)
					handleErr = err
					break
				}
				multiKvs = append(multiKvs, kvs)
			}
			if handleErr != nil {
				continue
			}
			fileName := fmt.Sprintf("mr-out-%d", taskInfo.TaskNum)
			if err := WriteKvsToFile(fileName, kWayMerge(multiKvs), reducef); err != nil {
				continue
			}
			notifyResp, notifyErr = NotifyTaskDone(workerID, taskInfo.TaskNum, taskInfo.TaskStage, nil)
		}
		if notifyErr != nil {
			log.Fatalf("notify task done failed: %v", notifyErr)
		}
		if notifyResp.ErrCode == ErrCode_WORKER_NOT_FOUND {
			workerID = InvalidWorkerID
			continue
		}
		if taskInfo.TaskStage == ReduceStage {
			// clean inter files
			if notifyResp.ErrCode == ErrCode_OK || notifyResp.ErrCode == ErrCode_ALL_DONE {
				removeFiles(taskInfo.Files)
			}
		}
		if notifyResp.ErrCode == ErrCode_ALL_DONE {
			return
		}
		if notifyResp.ErrCode != ErrCode_OK {
			log.Printf("notify task done failed: %v", notifyResp.ErrCode)
		}
	}
}

func ReadFile(fileName string) ([]byte, error) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Printf("cannot open %v", fileName)
		return nil, err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", fileName)
		return nil, err
	}
	file.Close()
	return content, nil
}

func WriteKvsToFile(fileName string, kvs []KeyValue,
	reducef func(string, []string) string) error {
	tempFileName := "tmp-" + fileName
	ofile, cErr := os.Create(tempFileName)
	if cErr != nil {
		log.Printf("cannot open %v", fileName)
		return cErr
	}
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)
		i = j
	}
	ofile.Close()
	if err := os.Rename(tempFileName, fileName); err != nil {
		log.Printf("rename file failed: %v", err)
		return err
	}
	return nil
}

func removeFiles(files []string) {
	for _, file := range files {
		os.Remove(file)
	}
}

func RegisterWorker() (*RegisterWorkerResponse, error) {
	args := &RegisterWorkerRequest{}
	reply := &RegisterWorkerResponse{}
	succ := call("Coordinator.RegisterWorker", args, reply)
	if !succ {
		return nil, fmt.Errorf("register worker failed")
	}
	return reply, nil
}

func GetTask(workerID int32) (*GetTaskResponse, error) {
	args := &GetTaskRequest{WokerID: workerID}
	reply := &GetTaskResponse{}
	succ := call("Coordinator.GetTask", args, reply)
	if !succ {
		return nil, fmt.Errorf("get task failed")
	}
	return reply, nil
}

func NotifyTaskDone(workerID int32, taskNum int, stage uint32,
	files []string) (*NotifyTaskDoneResponse, error) {
	args := &NotifyTaskDoneRequest{
		WokerID:           workerID,
		TaskNum:           taskNum,
		TaskStage:         stage,
		IntermediateFiles: files,
	}
	reply := &NotifyTaskDoneResponse{}
	succ := call("Coordinator.NotifyTaskDone", args, reply)
	if !succ {
		return nil, fmt.Errorf("notify task done failed")
	}
	return reply, nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Printf("failed to call rpc: %s, err: %s", rpcname, err.Error())
	return false
}
