package main

import (
	wm "job-dispatcher-golang/worker_manager"
)

func main() {
	// shouldImmediatelyQueryAgain := worker.DispatchOverdue(3)
	// fmt.Println(shouldImmediatelyQueryAgain)

	// worker.DispatchOverdue(9)
	// worker.ScanGroup2([]int{0, 2, 3, 5, 6, 7, 8, 9})
	// worker.ScanGroup()
	// worker.Tick()
	wm.StartScanSchedule()
}
