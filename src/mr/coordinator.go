package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type workerStatus int

const (
	idle = iota
	inProcess
	completed
	timewaiting
	kill
	Map
	Reduce
	mapPhase
	reducePhase
	allDone
)
const (
	TIMEOUT = time.Second * 15
)

type TaskType int
type Coordinator struct {
	// Your definitions here.
	mapChan             chan *Task
	reduceChan          chan *Task
	mapTasksNum         int           // number of map tasks
	reduceTasksNum      int           //number of reduce tasks
	currentPhaseTaskNum int           // 用于判断所有的任务是否都已经完成
	phase               int           // 当前任务的阶段
	detector            map[int]*Task // reduce 和 map都有，assign时分配
}

type FileInfo struct {
	Locations string
}

// 存放任务的id，状态
type Task struct {
	Status   workerStatus // 0 idle ;1 in-progress;2 completed,3 timewaiting, 4:kill
	Id       int
	Files    []FileInfo // input files of each task
	Type     TaskType   // map, reduce
	NReduce  int        // 用于map和reduce的hash配对
	ExpireAt time.Time  // 过期时间
}

// Your code here -- RPC handlers for the worker to call.
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

var detectorLock sync.RWMutex

/*分配map和reduce task*/
func (c *Coordinator) Assign(args *RPCArgs, repls *RPCReply) error {
	// 从Mapchannel里面拿map task, 如果map task 都完成之后再分配reduce task
	task := &repls.Task
	switch c.phase {
	case mapPhase:
		if len(c.mapChan) > 0 {
			*task = *<-c.mapChan
			task.Status = inProcess
			task.Type = Map
			c.update2Detector(task) // task 修改同步到coordinater中
		} else {
			// 拿不到task
			task.Status = timewaiting
			task.Type = Map
		}
		break
	case reducePhase:
		if len(c.reduceChan) > 0 {
			*task = *<-c.reduceChan
			task.Status = inProcess
			task.Type = Reduce
			c.update2Detector(task) // task 修改同步到coordinater中
		} else {
			task.Status = timewaiting
			task.Type = Reduce
		}
		break
	case allDone:
		// worker 接收到kill的task 之后退出程序
		task.Status = kill
	}
	c.TimeoutHandler() // coordinater 进行超时检测
	return nil
}

// map 和 reduce worker 执行完任务后调用该方法
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := c.phase == allDone
	return ret
}

var currentPhaseTaskNumLock sync.Mutex

func (c *Coordinator) OneWorkerDone(args *RPCArgs, repls *RPCReply) error {
	task := args.Task
	repls.Task = task
	phase := ""
	if c.phase == mapPhase {
		phase = "map phase"
	} else if c.phase == reducePhase {
		phase = "reduce phase"
	} else if c.phase == allDone {
		phase = "allDone"
	}
	log.Printf("%v finished, file:%v, current phase: %v and current task num is %v ", task.Id, task.Files, phase, c.currentPhaseTaskNum)
	log.Println()
	if task.Status != completed {
		// 如果所有的map task都执行完毕，进行Reduce Task的初始化
		// 需要把task的更新同步到runningtasks中
		currentPhaseTaskNumLock.Lock()
		c.currentPhaseTaskNum -= 1
		currentPhaseTaskNumLock.Unlock()
		task.Status = completed
		c.update2Detector(&task)
		if c.currentPhaseDone() {
			c.nextPhase()
		}
	}

	return nil
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	println("begin to make coordinator...")
	c := Coordinator{}
	// Your code here.
	c.mapTasksNum = len(files)
	c.reduceTasksNum = nReduce
	c.mapChan = make(chan *Task, c.mapTasksNum)
	// 要生成非阻塞管道
	c.reduceChan = make(chan *Task, c.reduceTasksNum)
	c.detector = make(map[int]*Task)
	c.makeMapTasks(files)
	println("finish MakeCoordinater task.")
	c.server()
	return &c
}

// 为reduce task 分配 输入文件 mr-X-i
func (c *Coordinator) getFileInput(reducei int) []FileInfo {
	infos := []FileInfo{}
	// reduce task 需要加上tmp路径
	prefix := "mr-"
	for i := 0; i < c.mapTasksNum; i++ {
		fileLoc := prefix + strconv.Itoa(i) + "-" + strconv.Itoa(reducei)
		info := FileInfo{Locations: fileLoc}
		infos = append(infos, info)
	}
	return infos
}

func (c *Coordinator) makeReduceTasks() {
	log.Printf("generate reduce tasks...")
	for i := 0; i < c.reduceTasksNum; i++ {
		c.reduceChan <- &Task{Status: idle, Id: i, Files: c.getFileInput(i), Type: Reduce, NReduce: c.reduceTasksNum, ExpireAt: time.Now().Add(TIMEOUT)}
	}
}

/*master 当前阶段任务是否完成*/
func (c *Coordinator) currentPhaseDone() bool {
	phaseLock.Lock()
	defer phaseLock.Unlock()
	return c.currentPhaseTaskNum == 0
}

var phaseLock sync.RWMutex

/*更改共享数据，需要加锁*/
func (c *Coordinator) nextPhase() {
	phaseLock.Lock()
	defer phaseLock.Unlock()
	if c.phase == mapPhase {
		c.currentPhaseTaskNum = c.reduceTasksNum
		c.phase = reducePhase
		c.makeReduceTasks()
		println("all map tasks done, change to reduce phase...")
	} else if c.phase == reducePhase {
		c.phase = allDone
		println("all reduce tasks done, prepare to close...")
	}
}

func (c *Coordinator) makeMapTasks(files []string) {
	log.Printf("begin to make map worker tasks.")
	// 一开始进行的是map阶段
	c.phase = mapPhase
	c.currentPhaseTaskNum = c.mapTasksNum
	for i := 0; i < c.mapTasksNum; i++ {
		// 生成M个map workers, 输入是split文件
		t := Task{Status: idle, Type: Map, Id: i, Files: []FileInfo{{Locations: files[i]}}, NReduce: c.reduceTasksNum, ExpireAt: time.Now().Add(TIMEOUT)}
		c.mapChan <- &t
		log.Println(t)
	}
}

/*用于crash test*/
/*每隔一段事件进行timeout检测，如果timeout，代表可能是宕机了，需要把这个任务再次放回到channel中*/
/*detector可能存在并发问题*/
func (c *Coordinator) TimeoutHandler() {
	detectorLock.Lock()
	defer detectorLock.Unlock()
	if c.phase == allDone {
		return
	}
	for _, task := range c.detector {
		if task.Type == Map && (task.Status == inProcess || task.Status == timewaiting) && task.ExpireAt.Before(time.Now()) {
			// 在TIMEOUT时间段中还没有完成，说明任务是宕机了，就算重新放入channel，由于id是一样的，文件会覆盖之前的内容，因此存在幂等性
			task.Status = idle
			task.ExpireAt = time.Now().Add(TIMEOUT)
			c.mapChan <- task
			log.Printf("restart map task ... %v %v", task.Id, task.Files)
		} else if task.Type == Reduce && (task.Status == inProcess || task.Status == timewaiting) && task.ExpireAt.Before(time.Now()) {
			task.Status = idle
			task.ExpireAt = time.Now().Add(TIMEOUT)
			c.reduceChan <- task
			log.Printf("restart reduce task ... %v", task.Id)
		}
	}
}

func (c *Coordinator) update2Detector(task *Task) {
	detectorLock.RLock()
	defer detectorLock.RUnlock()
	c.detector[task.Id] = task
}
