package main

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

func timedSimpleCall(nums []int) {
	start := time.Now()
	result := simpleSum1(nums)
	elapsed := time.Since(start)

	fmt.Printf("Sum: %v, Time: %v. No multithreading, range over slice values\n", result, elapsed)

	start = time.Now()
	result = simpleSum2(nums)
	elapsed = time.Since(start)

	fmt.Printf("Sum: %v, Time: %v. No multithreading, c style for loop\n", result, elapsed)

	start = time.Now()
	result = simpleSum3(nums)
	elapsed = time.Since(start)

	fmt.Printf("Sum: %v, Time: %v. No multithreading, c style for loop with length pre stored\n", result, elapsed)

	start = time.Now()
	result = simpleSum4(nums)
	elapsed = time.Since(start)

	fmt.Printf("Sum: %v, Time: %v. No multithreading, range over slice indices\n", result, elapsed)
}

func timedSlicedCall(nums []int, N int) {
	start := time.Now()
	result, batchSize := slicedSum(nums, N)
	elapsed := time.Since(start)

	fmt.Printf("Sum: %v, Num Threads: %v, Batch Size: %v, Time: %v\n", result, N, batchSize, elapsed)
}

func TestConcurrentSum(t *testing.T) {
	fmt.Println("Logical CPUs:", runtime.NumCPU())
	fmt.Println("GOMAXPROCS:", runtime.GOMAXPROCS(0))

	nums := make([]int, 100_000_000)
	for i := range nums {
		nums[i] = i + 1
	}
	timedSimpleCall(nums)
	timedSlicedCall(nums, 1)
	timedSlicedCall(nums, 2)
	timedSlicedCall(nums, 4)
	timedSlicedCall(nums, 8)
	timedSlicedCall(nums, 16)
	timedSlicedCall(nums, 32)
	timedSlicedCall(nums, 64)
}
