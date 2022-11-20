package worker

import (
	"fmt"
	"job-dispatcher-golang/metrics"
	ddbsvc "job-dispatcher-golang/services"
	"os"
	"os/signal"
	"time"
)

type Schedule = ddbsvc.Schedule

var PARTITIONS = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
var scanTimes = make(map[int]time.Time)

func init() {
	now := time.Now()
	for _, partition := range PARTITIONS {
		scanTimes[partition] = now
	}
}

func DispatchToDestination(schedule Schedule) {
	msg := fmt.Sprintf("== PUSH JOB TO DESTINATION QUEUE ==\n%s\n", schedule.JobSpec)
	fmt.Println(msg)
	ddbsvc.DeleteDispatchedJob(schedule)
}

func DispatchOverdue(partition int) bool {
	start := time.Now()
	queryResult := ddbsvc.GetOverdueJobs(partition)
	scheduleCount := len(queryResult.Schedules)
	done := make(chan bool, scheduleCount)
	for _, schedule := range queryResult.Schedules {
		go func(schedule Schedule, done chan bool) {
			schedule = ddbsvc.UpdateStatus(schedule, "SCHEDULED", "ACQUIRED")
			DispatchToDestination(schedule)
			done <- true
		}(schedule, done)
	}
	for i := 1; i <= scheduleCount; i++ {
		<-done
	}
	metrics.DispatchOverdue(time.Since(start), partition, scheduleCount)
	return queryResult.ShouldImmediatelyQueryAgian
}

func ScanGroup() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	ticker := time.NewTicker(time.Microsecond)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-ticker.C:
			ticker.Stop()
			start := time.Now()
			fmt.Println("start scanning....")
			noDelay := false
			for _, partition := range PARTITIONS {
				now := time.Now()
				if scanTimes[partition].Before(now) {
					queryImmediate := DispatchOverdue(partition)
					var nextScan time.Time
					if queryImmediate {
						nextScan = time.Now()
					} else {
						nextScan = time.Now().Add(time.Second)
					}
					scanTimes[partition] = nextScan
					noDelay = noDelay || queryImmediate
				} else {
					noDelay = false
				}
			}

			metrics.ScanGroup(time.Since(start), PARTITIONS, noDelay)
			var interval time.Duration
			if noDelay {
				interval = time.Millisecond
				// interval = time.Second
			} else {
				interval = time.Second
			}
			ticker.Reset(interval)
		case <-interrupt:
			fmt.Println("quit..")
			break loop
		}
	}
}

// optimistic lock 동시성 이슈....
func ScanGroup2(partitions []int) {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	ticker := time.NewTicker(time.Microsecond)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-ticker.C:
			ticker.Stop()
			fmt.Println("start scanning...")
			noDelay := false
			queryImmediate := make(chan bool, len(partitions))
			for _, partition := range partitions {
				go func(partition int, queryImmediate chan bool) {
					queryImmediate <- DispatchOverdue(partition)
				}(partition, queryImmediate)
			}
			for i := 1; i <= len(partitions); i++ {
				noDelay = noDelay || <-queryImmediate
			}
			fmt.Println("all partition scanning done...")
			var interval time.Duration
			if noDelay {
				interval = time.Millisecond
				// interval = time.Second
			} else {
				interval = time.Second
			}
			fmt.Println("new interval", interval)
			ticker.Reset(interval)
		case <-interrupt:
			break loop
		}
	}
}
