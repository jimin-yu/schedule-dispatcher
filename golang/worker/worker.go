package worker

import (
	"fmt"
	ddbsvc "job-dispatcher-golang/services"
)

type Schedule = ddbsvc.Schedule

func DispatchToDestination(schedule Schedule) {
	msg := fmt.Sprintf("== PUSH JOB TO DESTINATION QUEUE ==\n%s\n", schedule.JobSpec)
	fmt.Println(msg)
	ddbsvc.DeleteDispatchedJob(schedule)
}

func DispatchOverdue(partition int) bool {
	queryResult := ddbsvc.GetOverdueJobs(partition)
	scheduleCount := len(queryResult.Schedules)
	fmt.Println(scheduleCount)
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
	return queryResult.ShouldImmediatelyQueryAgian
}
