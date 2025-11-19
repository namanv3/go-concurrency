package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"time"
)

var (
	ErrPoolClosed    = errors.New("pool already closed, cannot accept new tasks")
	ErrPoolQueueFull = errors.New("pool queue is full, cannot accept new tasks")
)

// 1. Prime sieve up to ~200k
func PrimeSieve() {
	n := 200000
	sieve := make([]bool, n)
	for i := 2; i*i < n; i++ {
		if !sieve[i] {
			for j := i * i; j < n; j += i {
				sieve[j] = true
			}
		}
	}
}

// 2. Mandelbrot iteration sampling
func Mandelbrot() {
	maxIter := 500
	for py := 0; py < 200; py++ {
		for px := 0; px < 200; px++ {
			x0 := (float64(px)/200)*3.5 - 2.5
			y0 := (float64(py)/200)*2 - 1
			x, y := 0.0, 0.0
			iter := 0
			for x*x+y*y <= 4 && iter < maxIter {
				xt := x*x - y*y + x0
				y = 2*x*y + y0
				x = xt
				iter++
			}
		}
	}
}

// 3. Naive matrix multiplication (200x200)
func MatrixMultiply() {
	size := 200
	A := make([][]float64, size)
	B := make([][]float64, size)
	C := make([][]float64, size)

	for i := range A {
		A[i] = make([]float64, size)
		B[i] = make([]float64, size)
		C[i] = make([]float64, size)
		for j := 0; j < size; j++ {
			A[i][j] = float64(i + j)
			B[i][j] = float64(i - j)
		}
	}

	for i := 0; i < size; i++ {
		for j := 0; j < size; j++ {
			var sum float64
			for k := 0; k < size; k++ {
				sum += A[i][k] * B[k][j]
			}
			C[i][j] = sum
		}
	}
}

// 4. SHA-256 hashing in a tight loop
func HashStorm() {
	for i := 0; i < 20000; i++ {
		h := sha256.New()
		h.Write([]byte(fmt.Sprintf("iteration-%d", i)))
		_ = h.Sum(nil)
	}
}

// 5. JSON encode/decode stress
func JSONShuffle() {
	type Obj struct {
		A int
		B string
		C []int
	}

	for i := 0; i < 2000; i++ {
		o := Obj{
			A: i,
			B: "some moderately sized string used for testing JSON encode decode",
			C: []int{1, 2, 3, 4, 5, i},
		}
		b, _ := json.Marshal(o)
		var out Obj
		json.Unmarshal(b, &out)
	}
}

// 6. Random walk simulation
func RandomWalk() {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	const steps = 5_000_000
	x, y := 0, 0
	for i := 0; i < steps; i++ {
		switch rnd.Intn(4) {
		case 0:
			x++
		case 1:
			x--
		case 2:
			y++
		case 3:
			y--
		}
	}
	_ = x + y
}

// 7. Sorting a large slice
func SortLarge() {
	size := 600_000
	data := make([]int, size)
	for i := range data {
		data[i] = rand.Int()
	}
	sort.Ints(data)
}
