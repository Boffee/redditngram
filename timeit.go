package redditngram

import (
	"fmt"
	"time"

	"gonum.org/v1/gonum/stat"
)

const maxIter = 1e6
const maxDuration = time.Duration(10e9)
const scale = 2

func Timeit(f func()) {
	currMaxIter := 1
	var elapsed []float64
	var iterStart time.Time
	start := time.Now()
	i := 0
	for {
		if i >= maxIter || i >= currMaxIter && time.Since(start) > maxDuration/scale {
			break
		}
		iterStart = time.Now()
		f()
		elapsed = append(elapsed, time.Since(iterStart).Seconds())
		if i >= currMaxIter {
			currMaxIter *= scale
		}
		i++
	}
	mean := time.Duration(stat.Mean(elapsed, nil) * 1e9)
	stdDev := time.Duration(stat.StdDev(elapsed, nil) * 1e9)
	fmt.Printf("%s ± %s per loop (mean ± std. dev. of %d loops)\n", mean, stdDev, len(elapsed))
}
