package mapreduce

import (
	"container/list"
	"fmt"
	"sync"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

// runTask sends one DoJob RPC and reports whether the worker actually
// completed it. No retry here because the caller decides what to do on failure.
func (mr *MapReduce) runTask(worker string, args *DoJobArgs) bool {
	reply := DoJobReply{}
	ok := call(worker, "Worker.DoJob", args, &reply)
	return ok && reply.OK
}

// schedulePhase hands out nTasks jobs of the given operation to idle workers,
// retrying any task whose worker fails to complete it until every task has
// succeeded exactly once.
func (mr *MapReduce) schedulePhase(operation JobType, nTasks int, nOther int) {
	pending := make(chan int, nTasks)
	for i := 0; i < nTasks; i++ {
		pending <- i
	}

	var wg sync.WaitGroup
	wg.Add(nTasks)

	done := make(chan struct{})
	stopped := make(chan struct{})

	go func() {
		defer close(stopped)
		for {
			select {
			case taskNum := <-pending:
				worker := <-mr.idleWorkerChannel

				go func(taskNum int, worker string) {
					args := &DoJobArgs{
						File:          mr.file,
						Operation:     operation,
						JobNumber:     taskNum,
						NumOtherPhase: nOther,
					}
					if mr.runTask(worker, args) {
						mr.idleWorkerChannel <- worker // worker is free again
						wg.Done()                      // 1 task is done so making it done
					} else {
						fmt.Printf("schedulePhase: task %d failed on worker %s, retrying\n", taskNum, worker)
						pending <- taskNum // someone else will pick this task up
						// worker is presumed dead so we will put the task back in queue so that
						// some other free worker will pick it up
					}
				}(taskNum, worker)
			case <-done:
				return
			}
		}
	}()

	wg.Wait()
	close(done)
	// I am double checking it has because close(done) returning
	// doesn't mean the goroutine has processed it yet
	// the goroutine might still be mid-iteration doing something else
	// at that exact moment.
	// If the caller proceeded right after close(done) assuming the goroutine's gone,
	//  it could still be running concurrently for a brief window
	<-stopped
}

func (mr *MapReduce) RunMaster() *list.List {

	// identifying the idle workers so that we can assign them
	//either mapper or reducer task based on our workflow
	// once the worker is registered we are pusing it to register
	// channel so once that is trigger we will write it back to idle channel
	done := make(chan struct{})
	stopped := make(chan struct{})
	mr.Workers = make(map[string]*WorkerInfo)

	go func() {
		defer close(stopped)
		for {
			select {
			case worker := <-mr.registerChannel:
				mr.Workers[worker] = &WorkerInfo{address: worker}
				mr.idleWorkerChannel <- worker
			case <-done:
				return
			}
		}
	}()

	// map phase: retries any task whose worker fails until all nMap
	// tasks have succeeded
	mr.schedulePhase(Map, mr.nMap, mr.nReduce)

	// mapper is done and we will have individual smaller reducers
	// we will resuse the same workers and make them act as reducers
	mr.schedulePhase(Reduce, mr.nReduce, mr.nMap)

	// stop the registration forwarding goroutine and wait for it to
	// actually exit
	close(done)
	<-stopped

	return mr.KillWorkers()
}
