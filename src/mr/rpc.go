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

type TaskType int

const (
	Map    TaskType = 1
	Reduce TaskType = 2
	Done   TaskType = 3
)

// No arguments to send the coordinator to ask for a task
type GetTaskArgs struct{}
type GetTaskReply struct {
	// what type of task is this?
	TaskType TaskType

	// task number of either map or reduce task
	TaskNum int

	// needed for Map (to know which file to write)
	NReduceTasks int

	// needed for Map (to know which file to read)
	MapFile string

	// needed for Reduce (to know how many intermediate map files to read)
	NMapTasks int
}

// No arguments to send the coordinator to ask for a task
type FinishedTaskArgs struct {
	// what type of task is this?
	TaskType TaskType

	// task number of either map or reduce task
	TaskNum int
}

type FinishedTaskReply struct{}
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
