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
)

//
// Map functions return a slice of KeyValue.
//

type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	id int
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func getIntermediateFile(mapTaskN, redTaskN int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskN, redTaskN)
}

//
// finalize IntermidateFile atomically renames temp intermediate file
// to a completed intermediate task file
//
func finalizeIntermediateFile(tmpFilePath string, mapTaskN int, redTaskN int) {
	finalFilePath := getIntermediateFile(mapTaskN, redTaskN)
	os.Rename(tmpFilePath, finalFilePath)
}

func performMap(filename string, taskNum int, nReduceTasks int, mapf func(string, string) []KeyValue) {
	// read contents to map
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	file.Close()

	// apply map function to contents of file and collect
	// the set of key-value pairs
	kva := mapf(filename, string(content))

	// create temporary files and encoder for each file
	tmpFiles := []*os.File{}
	tmpFilenames := []string{}
	encoders := []*json.Encoder{}
	for r := 0; r < nReduceTasks; r++ {
		tmpFile, err := ioutil.TempFile("", "")
		if err != nil {
			log.Fatalf("cannot open temp files")
		}
		tmpFiles = append(tmpFiles, tmpFile)
		tmpFilenames = append(tmpFilenames, tmpFile.Name())
		encoder := json.NewEncoder(tmpFile)
		encoders = append(encoders, encoder)
	}

	// write output keys to appropriate (tmp) intermeidate files
	// using the provided ihash function
	for _, kv := range kva {
		r := ihash(kv.Key) % nReduceTasks
		encoders[r].Encode(&kv)
	}

	for _, f := range tmpFiles {
		f.Close()
	}

	// atomically rename tmp files to final intermediate files
	for r := 0; r < nReduceTasks; r++ {
		finalizeIntermediateFile(tmpFilenames[r], taskNum, r)
	}
}

func performReduce(taskNum, nReduceTasks int, reducef func(string, []string) string) {
	// get all intermediate files corresponding to this reduce task, and
	// collect the corresponding key-value pairs
	kva := []KeyValue{}
	for m := 0; m < nReduceTasks; m++ {
		iFilename := getIntermediateFile(m, taskNum)
		file, err := os.Open(iFilename)
		if err != nil {
			log.Fatalf("cannot open %v", iFilename)
		}
		decoder := json.NewDecoder(file)
		for decoder.More() {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	// sort the keys
	sort.Sort(ByKey(kva))

	//get temporary reduce file to write values
	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatalf("cannot open reduce tempFiles")
	}

	// apply reduce function once to all values of the same key
	key_begin := 0
	for key_begin < len(kva) {
		curr_Key := kva[key_begin].Key
		key_end := key_begin + 1

		for key_end < len(kva) && kva[key_end].Key == curr_Key {
			key_end++
		}

		values := []string{}
		for k := key_begin; k < key_end; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(curr_Key, values)

		fmt.Fprintf(tmpFile, "%v %v\n", curr_Key, output)

		key_begin = key_end
	}
	tmpFilename := tmpFile.Name()
	finalizeIntermediateFile(tmpFilename, taskNum, nReduceTasks)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}

		// this will wait until we get assigned a task!
		call("Coordinator.HandleGetTask", &args, &reply)

		// Your worker implementation here.
		switch reply.TaskType {
		case Map:
			performMap(reply.MapFile, reply.TaskNum, reply.NReduceTasks, mapf)
		case Reduce:
			performReduce(reply.TaskNum, reply.NMapTasks, reducef)
		case Done:
			os.Exit(0)
		default:
			fmt.Errorf("Bad task type? %s", reply.TaskType)
		}

		// tell coordinator that we're done
		finArgs := FinishedTaskArgs{
			TaskType: reply.TaskType,
			TaskNum:  reply.TaskNum,
		}
		finReply := FinishedTaskReply{}
		call("Coordinator.HandleFinishTask", &finArgs, &finReply)
		// uncomment to send the Example RPC to the coordinator.

	}
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
