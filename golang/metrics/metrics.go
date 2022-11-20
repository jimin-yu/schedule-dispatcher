package metrics

import (
	"fmt"
	"time"

	"github.com/fatih/color"
)

var (
	EMPTY          bool
	startTime      time.Time
	PARTITIONS     = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	partitionEmpty = make(map[int]bool)
	stopwatch      = color.New(color.FgWhite, color.BgGreen).SprintFunc()
	red            = color.New(color.Bold, color.FgRed).SprintFunc()
	blue           = color.New(color.Bold, color.FgBlue).SprintFunc()
	yellow         = color.New(color.Bold, color.FgYellow).SprintFunc()
	magenta        = color.New(color.Bold, color.FgMagenta).SprintFunc()
)

func init() {
	for _, partition := range PARTITIONS {
		partitionEmpty[partition] = false
	}
	EMPTY = false
	startTime = time.Now()
	fmt.Printf("== %s ==", yellow(startTime))
}

func GetOverdueJobs(elapsedTime time.Duration, partition int) {
	fmt.Printf("[%s] %s : retreive overdue schedules from partition %d\n", red("GetOverdueJobs"), stopwatch(elapsedTime), partition)
}

func DispatchOverdue(elapsedTime time.Duration, partition int, count int) {
	if !EMPTY && count == 0 {
		tableEmpty(partition)
	}
	fmt.Printf("[%s] %s : fetch %d schedules(s) and dispatch to destination from partition %d\n", blue("DispatchOverdue"), stopwatch(elapsedTime), partition, count)
}

func ScanGroup(elapsedTime time.Duration, partitions []int, scanImmediately bool) {
	fmt.Printf("[%s] %s : iterate all %d partitions once. start next scan immediately = %t\n", magenta("ScanGroup"), stopwatch(elapsedTime), len(partitions), scanImmediately)
}

func tableEmpty(partition int) {
	partitionEmpty[partition] = true
	allEmpty := true
	for _, partition := range PARTITIONS {
		allEmpty = allEmpty && partitionEmpty[partition]
	}
	if allEmpty {
		EMPTY = true
		fmt.Printf("[%s] %s : table is empty\n", yellow("TableEmpty"), stopwatch(time.Since(startTime)))
	}
}
