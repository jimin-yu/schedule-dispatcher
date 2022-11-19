package main

import (
	"fmt"
	worker "job-dispatcher-golang/worker"
)

func main() {
	shouldImmediatelyQueryAgain := worker.DispatchOverdue(3)
	fmt.Println(shouldImmediatelyQueryAgain)
}
