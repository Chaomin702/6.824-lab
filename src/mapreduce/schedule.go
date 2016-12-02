package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	nworks := len(mr.workers)
	if phase == mapPhase {
		for i := 0; i < nworks; i++ {
			fmt.Println("work name ", <-mr.registerChannel)
		}
	}
	taskBelongs := make([][]int, nworks)
	for i := 0; i < ntasks; i++ {
		taskBelongs[i%nworks] = append(taskBelongs[i%nworks], i)
	}
	chs := make([]chan int, nworks)

	f := func(srv string, tasks []int, ch chan int) {
		for _, v := range tasks {
			taskArgs := new(DoTaskArgs)
			taskArgs.File = mr.files[v]
			taskArgs.JobName = mr.jobName
			taskArgs.NumOtherPhase = nios
			taskArgs.Phase = phase
			taskArgs.TaskNumber = v
			call(srv, "Worker.DoTask", &taskArgs, new(struct{}))
		}
		ch <- 1
	}
	for i := 0; i < nworks; i++ {
		chs[i] = make(chan int)
		go f(mr.workers[i], taskBelongs[i], chs[i])
	}
	for _, ch := range chs {
		<-ch
	}
	//taskArgs := new(DoTaskArgs)

	fmt.Printf("Schedule: %v phase done\n", phase)
}
