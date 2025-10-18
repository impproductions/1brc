package main

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Stat struct {
	Min   float64
	Tot   float64
	Count int64
	Max   float64
}

var stats map[string]*Stat = make(map[string]*Stat, 0)
var lc atomic.Int64

func ProcessChunks(f *os.File, chunkSize int64, parallelism int64) int {
	info, err := f.Stat()
	if err != nil {
		panic("couldn't retreive file stats")
	}
	r := f
	size := info.Size()
	tot := 0
	permit := struct{}{}
	var wg sync.WaitGroup
	var offset int64
	semaphoreChan := make(chan struct{}, parallelism)
	ctx, canc := context.WithCancel(context.Background())
	localStats := make(map[string]*Stat, 0)
	defer canc()

	fmt.Printf("Size: %v\n", size)
	for {
		select {
		case semaphoreChan <- permit:
			wg.Add(1)
			go func(o int64) {
				defer wg.Done()
				computedStats, err := ProcessChunk(r, o, chunkSize)
				if err != nil && err != io.EOF {
					panic(err)
				}
				if err == io.EOF {
					canc()
				}
				<-semaphoreChan
				localStats = computedStats
				if offset > size {
					canc()
				}
			}(offset)
			for k, v := range localStats {
				if val, exists := stats[k]; exists {
					stats[k].Min = min(val.Min, v.Min)
					stats[k].Tot = val.Tot + v.Tot
					stats[k].Count = val.Count + v.Count
					stats[k].Max = max(val.Max, v.Max)
				} else {
					stats[k] = v
				}
			}
			tot++
			offset += chunkSize
		case <-ctx.Done():
			wg.Wait()

			return tot
		}
	}
}

func ProcessChunk(r io.ReaderAt, from, bytes int64) (map[string]*Stat, error) {
	extra := int64(64)
	buf := make([]byte, bytes+extra)

	read, err := r.ReadAt(buf, from)
	if err != nil && err != io.EOF {
		panic(err)
	}

	if read == 0 && err == io.EOF {
		return nil, err
	}

	localStats := make(map[string]*Stat, 0)

	// fmt.Printf("Chunk %d[%d] %s-%s\n", from, from+bytes, string(buf[0:5]), string(buf[read-6:read-1]))

	// localStats := make(map[string][]float64, 0)
	currentCity := []byte{}
	currentTemp := []byte{}
	leftOfSemicolon := true
	isFirstLine := true
	if from == 0 {
		isFirstLine = false
	}
	lines := 0
	for p := range bytes + extra {
		c := buf[p]
		if isFirstLine {
			if c == '\n' {
				isFirstLine = false
			}
			continue
		}
		if p > min(int64(read), bytes) {
			if buf[p-1] == '\n' {
				// we've overshot and are done processing the last line of the chunk
				break
			}
		}
		if c == ';' {
			leftOfSemicolon = false
			continue
		}
		if c == '\n' && p <= int64(read)-1 {
			leftOfSemicolon = true
			parsed, err := strconv.ParseFloat(string(currentTemp), 64)
			if err != nil {
				panic(fmt.Sprintf("couldn't parse float: %s;%s", currentCity, currentTemp))
			}

			_, found := localStats[string(currentCity)]
			if !found {
				localStats[string(currentCity)] = &Stat{
					Min: parsed,
					Tot: 0,
					Max: parsed,
				}
			}

			localStats[string(currentCity)].Min = min(localStats[string(currentCity)].Min, parsed)
			localStats[string(currentCity)].Tot = localStats[string(currentCity)].Tot + parsed
			localStats[string(currentCity)].Count++
			localStats[string(currentCity)].Max = max(localStats[string(currentCity)].Max, parsed)

			currentCity = []byte{}
			currentTemp = []byte{}
			lines += 1
			continue
		}

		if leftOfSemicolon {
			currentCity = append(currentCity, c)
		} else {
			currentTemp = append(currentTemp, c)
		}
	}

	lc.Add(int64(lines))

	return localStats, err
}

func ceilTo(n float64, decimals int) float64 {
	factor := math.Pow(10, float64(decimals))
	return math.Ceil(n*factor) / factor
}

func main() {
	const chunkSize = 4194304
	const parallelism = 8
	const filePath = "assets/measurements_max.txt"

	start := time.Now()
	fmt.Printf("Starting at %s\n", start)
	f, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	totSections := ProcessChunks(f, chunkSize, parallelism)

	fmt.Printf("{")
	keys := make([]string, len(stats))
	cnt := 0
	for k := range stats {
		keys[cnt] = k
		cnt++
	}
	sort.Strings(keys)
	cnt = 0
	for _, k := range keys {
		v := stats[k]
		v.Min = ceilTo(v.Min, 1)
		v.Max = ceilTo(v.Max, 1)
		fmt.Printf("%s=%.1f/%.1f/%.1f", k, v.Min, ceilTo(v.Tot/float64(v.Count), 1), v.Max)
		if cnt < len(keys)-1 {
			fmt.Printf(", ")
		}
		cnt++
	}
	fmt.Printf("}\n")

	fmt.Printf("Done in %s\n", time.Since(start))
	fmt.Printf("Done! chunks: %d, lines: %d\n", totSections, lc.Load())
}
