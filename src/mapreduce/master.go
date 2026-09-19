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

func (mr *MapReduce) RPCMasterToWorker(worker string, args DoJobArgs, wg *sync.WaitGroup) {
	defer wg.Done()
	reply := DoJobReply{}
	ok := call(worker, "Worker.DoJob", args, &reply)
	if ok == false {
		fmt.Printf("RPC error: RPC from master to worker %s  error\n", worker)
	}
	if !reply.OK {
		fmt.Println("the worker has responded not okay to the master")
	} else { // successful so worker job is done and its idle again
		mr.idleWorkerChannel <- worker
	}
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

	var wg sync.WaitGroup
	for i := 0; i < mr.nMap; i++ {
		// we will keep assigning the various chunks
		//  to various available worker to map parallelly
		idleWorker := <-mr.idleWorkerChannel
		wg.Add(1)

		go mr.RPCMasterToWorker(idleWorker, DoJobArgs{
			File:          mr.file,
			Operation:     Map,
			JobNumber:     i,
			NumOtherPhase: mr.nReduce,
		}, &wg)
	}

	// wait group to wait for
	// all of the map to done to start reduce
	wg.Wait()

	// mapper is done and we will have individual smaller reducers
	// we will resuse the same workers and make them act as reducers
	for i := 0; i < mr.nReduce; i++ {
		idleWorker := <-mr.idleWorkerChannel
		wg.Add(1)

		go mr.RPCMasterToWorker(idleWorker, DoJobArgs{
			File:          mr.file,
			Operation:     Reduce,
			JobNumber:     i,
			NumOtherPhase: mr.nMap,
		}, &wg)
	}
	// waiting for all reducers to be done
	wg.Wait()

	// stop the registration forwarding goroutine and wait for it to
	// actually exit
	close(done)
	<-stopped

	return mr.KillWorkers()
}
