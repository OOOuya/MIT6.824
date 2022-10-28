package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"time"
)
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// ByKey 组合了sort
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var SLEEPTIME = time.Second * 1

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// 一定要轮询，一个worker不是只执行一次task
	done := false
	println("begin worker")
	for !done {
		task := CallCoordinater2AssignTask()
		switch task.Status {
		case inProcess:
			switch task.Type {
			case Map:
				domap(task, mapf)
				break
			case Reduce:
				doReduce(task, reducef)
				break
			}
			break
		case timewaiting:
			// 如果是timewaiting的时候，worker需要sleep一段时间吗
			//log.Printf("task is waiting for 1 sec..")
			time.Sleep(SLEEPTIME)
			break
		case kill:
			done = true
			log.Println("task prepare to close...")
			break
		default:
			log.Printf("can not interpret task type...")
		}
	}
}

func doReduce(reduceTask Task, reducef func(string, []string) string) {
	log.Printf("do reduce task: %v. input files: %v", reduceTask.Id, reduceTask.Files)
	// 接受中间文件， 进行reducef
	filenames := []string{}
	for _, file := range reduceTask.Files {
		filenames = append(filenames, file.Locations)
	}
	// 解析每个文件的json
	kvs := []KeyValue{}
	for _, filename := range filenames {
		data, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}
		tmpKv := KeyValue{}
		decoder := json.NewDecoder(data)
		for {
			err := decoder.Decode(&tmpKv)
			if err != nil {
				break
			}
			kvs = append(kvs, tmpKv)
		}
		err = data.Close()
		if err != nil {
			log.Fatal(err)
		}
	}
	// 生成输出文件
	fname := "mr-out-" + strconv.Itoa(reduceTask.Id)
	tempF, err := os.CreateTemp(".", fname)
	if err != nil {
		log.Fatal(err)
		return
	}
	sort.Sort(ByKey(kvs))
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
		// 进行reduce操作
		output := reducef(kvs[i].Key, values)
		//log.Println(kvs[i].Key, values, output)
		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(tempF, "%v %v\n", kvs[i].Key, output)
		if err != nil {
			log.Fatal(err)
			return
		}
		i = j
	}
	// rename the file atomically
	err = os.Rename(tempF.Name(), fname)
	if err != nil {
		log.Fatal(err)
	}
	callTaskDone(reduceTask)
}

func domap(task Task, mapf func(string, string) []KeyValue) {
	log.Printf("do map task: %v. input files: %v", task.Id, task.Files)
	if len(task.Files) == 0 {
		log.Fatal("map task doesnt have corresponding file...")
	}
	keyValues := []KeyValue{}
	for _, taskFile := range task.Files {
		inputFilePath := taskFile.Locations
		file, err := os.Open(inputFilePath)
		if err != nil {
			log.Fatal("Map worker cannot open input files!")
		}
		inputFile, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatal("Map worker cannot read input files!")
		}
		kva := mapf(inputFilePath, string(inputFile))
		keyValues = append(keyValues, kva...)
	}

	// 将keyValues存入local disk 中, keyvalues排序， 相同key 拼接
	sort.Sort(ByKey(keyValues))
	formattedKV := [][]KeyValue{}
	for i := 0; i < task.NReduce; i++ {
		formattedKV = append(formattedKV, []KeyValue{})
	}
	for _, kv := range keyValues {
		idx := ihash(kv.Key) % task.NReduce
		formattedKV[idx] = append(formattedKV[idx], kv)
	}
	prefix := "mr-" + strconv.Itoa(task.Id)
	// 一个reduce 对应一个文件, 非原子操作, 将kv以json形式存入
	for i := 0; i < task.NReduce; i++ {
		filename := prefix + "-" + strconv.Itoa(i)
		file, err := os.Create(filename)
		if nil != err {
			log.Fatal("map worker fails to create a intermediate file!", err)
		}
		enc := json.NewEncoder(file)
		for _, kv := range formattedKV[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("Intermediate kvs fail to write into files!")
			}
		}
	}
	// 通知master map task完成，告知临时文件的位置和task状态
	callTaskDone(task)
}

func callTaskDone(task Task) {
	args := RPCArgs{task}
	reply := RPCReply{}
	ok := call("Coordinator.OneWorkerDone", &args, &reply)
	if !ok {
		log.Fatal("call failed!\n")
	}
}

/*调用Coordinater，把map task 交付给worker*/
func CallCoordinater2AssignTask() (mapTask Task) {
	args := RPCArgs{Task{Type: Map}}
	reply := RPCReply{}
	ok := call("Coordinator.Assign", &args, &reply)
	if !ok {
		log.Fatal("call failed!\n")
	}
	mapTask = reply.Task
	return
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	fmt.Println(err)
	return false
}
