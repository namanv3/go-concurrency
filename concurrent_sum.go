package main

import (
	"sync"
)

func simpleSum1(slice []int) int {
	sum := 0
	for _, num := range slice {
		sum += num
	}
	return sum
}

func simpleSum2(slice []int) int {
	sum := 0
	for i := 0; i < len(slice); i++ {
		sum += slice[i]
	}
	return sum
}

func simpleSum3(slice []int) int {
	sum := 0
	N := len(slice)
	for i := 0; i < N; i++ {
		sum += slice[i]
	}
	return sum
}

func simpleSum4(slice []int) int {
	sum := 0
	for i := range slice {
		sum += slice[i]
	}
	return sum
}

func slicedSum(slice []int, N int) (int, int) {
	numJobs := len(slice)
	batchSize := numJobs / N

	partialResults := make(chan int, N)
	var wg sync.WaitGroup

	for startIdx := 0; startIdx < numJobs; startIdx += batchSize {
		wg.Add(1)
		s := startIdx // not needed after go version 1.22
		e := min((s + batchSize), numJobs)
		go func() {
			defer wg.Done()
			sum := 0
			for i := s; i < e; i++ {
				sum += slice[i]
			}
			partialResults <- sum
		}()
	}

	go func() {
		wg.Wait()
		close(partialResults)
	}()

	total := 0
	for partialSum := range partialResults {
		total += partialSum
	}
	return total, batchSize
}
