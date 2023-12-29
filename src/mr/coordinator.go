package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	// Allow coordinator to wait to assign reduce tasks until map tasks have finished,
	// or when all tasks are assigned and are running.
	// The coordinator is woken up either when a task has finished, or if a timeout has expired.
	cond *sync.Cond

	// len(mapFiles) == nMap
	mapFiles     []string
	nMapTasks    int
	nReduceTasks int

	// Keep track of when taskes are assigned,
	// and which tasks have finished
	mapTasksFinished    []bool
	mapTasksIssued      []time.Time
	reduceTasksFinished []bool
	reduceTasksIssued   []time.Time

	// set to true when all reduce tasks are complete
	isDone bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) HandleGetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NReduceTasks = c.nReduceTasks
	reply.NMapTasks = c.nMapTasks

	// issue all map tasks
	for {
		mapDone := true
		for m, done := range c.mapTasksFinished {
			if !done {
				// Assign a task if it's either never been issued, or if it's been too long
				// since it was issued to worker may have crashed
				// note: if task has never been issued, time is initialized to 0 UTC
				if c.mapTasksIssued[m].IsZero() || time.Since(c.mapTasksIssued[m]).Seconds() > 10 {
					reply.TaskType = Map
					reply.TaskNum = m
					reply.MapFile = c.mapFiles[m]
					c.mapTasksIssued[m] = time.Now()
					return nil
				} else {
					mapDone = false
				}
			}
		}

		// if all maps are in progress and haven't time out, wait to give another task
		if !mapDone {
			c.cond.Wait()
		} else {
			// we're done with all map tasks
			break
		}
	}

	// issue reduce tasks
	for {
		reduceDone := true
		for r, done := range c.reduceTasksFinished {
			if !done {
				// Assign a task if it's either never been issued, or if it's been too long
				// since it was issued to worker may have crashed
				// note: if task has never been issued, time is initialized to 0 UTC
				if c.reduceTasksIssued[r].IsZero() || time.Since(c.reduceTasksIssued[r]).Seconds() > 10 {
					reply.TaskType = Reduce
					reply.TaskNum = r
					c.reduceTasksIssued[r] = time.Now()
					return nil
				} else {
					reduceDone = false
				}
			}
		}

		// if all maps are in progress and haven't time out, wait to give another task
		if !reduceDone {
			c.cond.Wait()
		} else {
			// we're done with all map tasks
			break
		}
	}
	// if all map and reduce tasks are done, send the querying worker
	// a Done TaskType, and set isDone to true
	reply.TaskType = Done
	c.isDone = true

	return nil
}

func (c *Coordinator) HandleFinishTask(args *FinishedTaskArgs, reply *FinishedTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case Map:
		c.mapTasksFinished[args.TaskNum] = true
	case Reduce:
		c.reduceTasksFinished[args.TaskNum] = true
	default:
		log.Fatalf("Bad finished task? %s", args.TaskType)
	}

	// wake up GetTask handler loop: a task has finished, so we might be able to assign another one
	c.cond.Broadcast()

	return nil
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
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.isDone
}

//
// wake up the GetTask handler thread every once in a while to check if some tasks
// hasn't finished, so we can know to reissue it
//
func (c *Coordinator) loop() {
	for {
		c.mu.Lock()
		c.cond.Broadcast()
		c.mu.Unlock()
		time.Sleep(time.Second)
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{}

	c.cond = sync.NewCond(&c.mu)

	c.mapFiles = files
	c.nMapTasks = len(files)
	c.mapTasksFinished = make([]bool, len(files))
	c.mapTasksIssued = make([]time.Time, len(files))

	c.nReduceTasks = nReduce
	c.reduceTasksFinished = make([]bool, nReduce)
	c.reduceTasksIssued = make([]time.Time, nReduce)
	go c.loop()

	c.server()
	return &c
}
