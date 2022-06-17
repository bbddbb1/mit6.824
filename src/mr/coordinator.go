package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Task struct {
	Name      string //任务名字
	Type      int    ////任务类别，0:map,1:reduce,2:sleep
	Status    int    //任务状态，正常0或者超时1
	mFileName string //如果是map任务，则记录分配给该任务的文件名字
	rFileName int    //如果是reduce任务，则记录分配给该任务的文件组编号
}

type Coordinator struct {
	// Your definitions here.
	map_record    map[string]int   //0表示未执行，1表示正在执行,2表示已经完成
	reduce_record map[int]int      //0表示未执行，1表示正在执行,2表示已经完成
	reduce_files  map[int][]string //记录中间文件
	map_count     int
	reduce_count  int
	mu            sync.Mutex
	map_finished  bool
	reduce_num    int
	task_pool     map[string]*Task
	num_task      int
}

const (
	Map = iota
	Reduce
	Sleep
)
const (
	Working = iota
	Timeout
)
const (
	NotStarted = iota
	Processing
	Finished
)

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskRequest, reply *GetTaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.RFileName = make([]string, 0)
	reply.ReduceNumber = c.reduce_num
	reply.MFileName = ""
	reply.TaskName = strconv.Itoa(c.num_task)
	c.num_task += 1
	if c.map_finished {
		for task := range c.reduce_record {
			flag := c.reduce_record[task]
			if flag == Processing || flag == Finished {
				continue
			}
			c.reduce_record[task] = Processing
			//for _, filename := range c.reduce_files[task] {
			//	reply.RFileName = append(reply.RFileName, filename)
			//}
			reply.RFileName = append(reply.RFileName, c.reduce_files[task]...)
			reply.TaskType = Reduce
			t := &Task{reply.TaskName, reply.TaskType, Working, "", task}
			c.task_pool[reply.TaskName] = t
			go c.HandleTimeout(reply.TaskName)
			return nil
		}
		reply.TaskType = Sleep
		return nil
	} else {
		for task, task_state := range c.map_record {
			if task_state == Processing || task_state == Finished {
				continue
			}
			c.map_record[task] = Processing
			reply.TaskType = Map
			reply.MFileName = task
			t := &Task{reply.TaskName, reply.TaskType, Working, reply.MFileName, -1}
			c.task_pool[reply.TaskName] = t
			go c.HandleTimeout(reply.TaskName)
			return nil
		}
		reply.TaskType = Sleep
		return nil
	}
}
func (c *Coordinator) Report(args *ReportStatusRequest, reply *ReportStatusResponse) error {
	reply.X = 1
	c.mu.Lock()
	defer c.mu.Unlock()
	if t, ok := c.task_pool[args.TaskName]; ok {
		flag := t.Status
		if flag == Timeout {
			delete(c.task_pool, args.TaskName)
			return nil
		}
		if t.Type == Map {
			c.map_count += 1
			c.map_record[t.mFileName] = Finished
			if c.map_count == len(c.map_record) {
				c.map_finished = true
			}
			for _, file := range args.FilesName {
				index := strings.LastIndex(file, "_")
				num, err := strconv.Atoi(file[index+1:])
				if err != nil {
					log.Fatal(err)
				}
				c.reduce_files[num] = append(c.reduce_files[num], file)
			}
			return nil
		} else if t.Type == Reduce {
			c.reduce_count += 1
			c.reduce_record[t.rFileName] = Finished
			delete(c.task_pool, t.Name)
			return nil
		} else {
			log.Fatal(" not in Task Type")
			return nil
		}
	}
	log.Println(args.TaskName, " task is not in Master record")
	return nil
}
func (c *Coordinator) HandleTimeout(taskName string) {
	time.Sleep(time.Second * 10)
	c.mu.Lock()
	defer c.mu.Unlock()
	if t, ok := c.task_pool[taskName]; ok {
		t.Status = Timeout
		if t.Type == Map {
			if c.map_record[t.mFileName] == Processing {
				c.map_record[t.mFileName] = NotStarted
			}
		} else if t.Type == Reduce {
			if c.reduce_record[t.rFileName] == Processing {
				c.reduce_record[t.rFileName] = NotStarted
			}
		}
	}
}

//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reduce_num == c.reduce_count {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		map_record:    make(map[string]int),   //记录需要map的文件，0表示未执行，1表示正在执行,2表示已经完成
		reduce_record: make(map[int]int),      //记录需要reduce的文件，0表示未执行，1表示正在执行,2表示已经完成
		reduce_files:  make(map[int][]string), //记录中间文件
		task_pool:     make(map[string]*Task),
		map_count:     0,            //记录已经完成map的任务数量
		reduce_count:  0,            //记录已经完成的reduce的任务数量
		map_finished:  false,        //
		reduce_num:    nReduce,      //需要执行的reduce的数量
		mu:            sync.Mutex{}, //
	}

	// Your code here.
	for _, file := range files {
		c.map_record[file] = 0
	}
	for i := 0; i < nReduce; i++ {
		c.reduce_record[i] = 0
	}
	c.server()
	return &c
}
