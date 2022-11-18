package main

import (
	"fmt"
	ddbsvc "job-dispatcher-golang/services"
	"time"
)

func main() {
	now := time.Now().UnixMilli()
	queryresult := ddbsvc.GetOverdueJobs(0, now, "SCHEDULED")
	schedule := queryresult.Schedules[0]
	fmt.Println(schedule)
	schedule = ddbsvc.UpdateStatus(schedule, "SCHEDULED", "ACQUIRED")
	fmt.Println(schedule)
}
