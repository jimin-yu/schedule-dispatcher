package main

import (
	"fmt"
	ddbsvc "job-dispatcher-golang/services"
	"time"
)

func main() {
	now := time.Now().UnixMilli()
	queryresult := ddbsvc.GetOverdueJobs(9, now, "SCHEDULED")
	fmt.Println(queryresult.ShouldImmediatelyQueryAgian)
}
