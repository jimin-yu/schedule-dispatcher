package wm

import (
	"job-dispatcher-golang/worker"
	"runtime"
	"time"
)

var numWorkers = runtime.NumCPU()
var PARTITIONS = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

type Result struct {
	partition         int
	nextScanImmediate bool
}

func createWorker(w int, tasks <-chan int, results chan<- Result) {
	for partition := range tasks {
		results <- Result{partition, worker.DispatchOverdue(partition)}
	}
}

func StartScanSchedule() {
	tasks := make(chan int)
	results := make(chan Result)

	// create workers
	for w := 1; w <= numWorkers; w++ {
		go createWorker(w, tasks, results)
	}
	// submit tasks
	for _, p := range PARTITIONS {
		tasks <- p
	}

	// waiting for result, add new tasks
	for result := range results {
		go func(result Result) {
			if !result.nextScanImmediate {
				time.Sleep(time.Second)
			}
			tasks <- result.partition
		}(result)
	}
}

func StartScanCronJob() {
	// job 테이블 스캔 => 스케줄 테이블에 넣기
}

// main.go
// func main() {
// 	wm.StartScanSchedule()
// }
