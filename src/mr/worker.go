package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func HandleMap(mapf func(string, string) []KeyValue, filename string, filenum int, tasknum string) []string {
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("cannot open ", file)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("cannot read ", file)
	}
	defer file.Close()
	key_value := mapf(filename, string(content))
	intermediate = append(intermediate, key_value...)
	filenames := make([]string, filenum)
	files := make([]*os.File, filenum)

	for i := 0; i < filenum; i++ {
		oname := "mr"
		oname = oname + "_" + tasknum + "_" + strconv.Itoa(i)
		//log.Println("create ",oname)
		ofile, _ := os.Create(oname)
		files[i] = ofile
		filenames[i] = oname
	}
	for _, kv := range intermediate {
		index := ihash(kv.Key) % filenum
		enc := json.NewEncoder(files[index])
		enc.Encode(&kv)
	}
	return filenames

}
func HandleReduce(reducef func(string, []string) string, filenames []string) string {
	files := make([]*os.File, len(filenames))
	intermediate := []KeyValue{}
	for i, filename := range filenames {
		var err error
		files[i], err = os.Open(filename)
		if err != nil {
			log.Fatalln("connot open ", filename)
		}
		kv := KeyValue{}
		decoder := json.NewDecoder(files[i])
		for {
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-"
	index := filenames[0][strings.LastIndex(filenames[0], "_")+1:]
	oname = oname + index
	ofile, error := os.Create(oname)
	if error != nil {
		log.Fatal("connot create file: ", oname)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	return oname
}
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := GetTaskRequest{}
		args.X = 0
		rep := GetTaskResponse{}
		call("Coordinator.GetTask", &args, &rep)
		if rep.TaskType == Map {
			filename := HandleMap(mapf, rep.MFileName, rep.ReduceNumber, rep.TaskName)
			rep_args := ReportStatusRequest{filename, rep.TaskName}
			rep_reply := ReportStatusResponse{1}
			call("Coordinator.Report", &rep_args, &rep_reply)
		} else if rep.TaskType == Reduce {
			HandleReduce(reducef, rep.RFileName)
			filename := make([]string, 0)
			rep_args := ReportStatusRequest{filename, rep.TaskName}
			rep_reply := ReportStatusResponse{1}
			call("Coordinator.Report", &rep_args, &rep_reply)
		} else if rep.TaskType == Sleep {
			time.Sleep(time.Millisecond)
		} else {
			log.Fatal("task type error")
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
