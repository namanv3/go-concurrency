package main

import (
	"bufio"
	"compress/gzip"
	"io"
	"sync"
)

func streamReader[T any](r io.Reader, parse func([]byte) T, N int) <-chan T {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil
	}
	defer gz.Close()

	type Job struct {
		idx         int
		jsonToParse []byte
	}
	jsonsToParse := make(chan Job)
	go func() {
		scanner := bufio.NewScanner(gz)
		i := 0
		for scanner.Scan() {
			json := scanner.Bytes()
			jsonsToParse <- Job{idx: i, jsonToParse: json}
			i++
		}
		close(jsonsToParse)
	}()

	var wg sync.WaitGroup
	type ResWithIdx struct {
		idx int
		res T
	}
	unsequencedResults := make(chan ResWithIdx)
	for range N {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jsonsToParse {
				result := parse(job.jsonToParse)
				unsequencedResults <- ResWithIdx{idx: job.idx, res: result}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(unsequencedResults)
	}()

	sequencedResults := make(chan T)
	go func() {
		defer close(sequencedResults)
		waitingLine := map[int]T{}
		next := 0
		for resultWithIdx := range unsequencedResults {
			waitingLine[resultWithIdx.idx] = resultWithIdx.res
			for {
				nextRes, ok := waitingLine[next]
				if !ok {
					break
				}
				sequencedResults <- nextRes
				delete(waitingLine, next)
				next++
			}
		}
		for {
			nextRes, ok := waitingLine[next]
			if !ok {
				break
			}
			sequencedResults <- nextRes
			delete(waitingLine, next)
			next++
		}
	}()

	return sequencedResults
}
