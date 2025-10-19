package main

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"runtime/pprof"
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
	// currentCity := []byte{}
	// currentTemp := []byte{}
	cityStart, cityPointer := int64(0), int64(0)
	tempStart, tempPointer := int64(0), int64(0)
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
				cityStart = p + 1
				cityPointer = p + 1
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
			tempStart = p + 1
			tempPointer = p + 1
			continue
		}
		if c == '\n' && p <= int64(read)-1 {
			currentCity := string(buf[cityStart:cityPointer])
			currentTemp := string(buf[tempStart:tempPointer])
			leftOfSemicolon = true
			parsed, err := strconv.ParseFloat(currentTemp, 64)
			if err != nil {
				panic(fmt.Sprintf("couldn't parse float: %s;%s", currentCity, currentTemp))
			}

			val, found := localStats[currentCity]
			if !found {
				val = &Stat{
					Min: parsed,
					Tot: 0,
					Max: parsed,
				}
				localStats[currentCity] = val
			}

			val.Min = min(val.Min, parsed)
			val.Tot = val.Tot + parsed
			val.Count++
			val.Max = max(val.Max, parsed)

			// currentCity = []byte{}
			cityStart = p + 1
			cityPointer = p + 1

			// currentTemp = []byte{}
			lines += 1
			continue
		}

		if leftOfSemicolon {
			cityPointer++
			// currentCity = append(currentCity, c)
		} else {
			tempPointer++
			// currentTemp = append(currentTemp, c)
		}
	}

	return localStats, err
}

func ceilTo(n float64, decimals int) float64 {
	factor := math.Pow(10, float64(decimals))
	return math.Ceil(n*factor) / factor
}

func main() {
	// CPU profiling
	cpuProfile, err := os.Create("cpu.prof")
	if err != nil {
		panic(err)
	}
	defer cpuProfile.Close()
	if err := pprof.StartCPUProfile(cpuProfile); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()

	chunkSize := int64(4194304)
	if len(os.Args) > 1 {
		cs, err := strconv.Atoi(os.Args[1])
		chunkSize = int64(cs)
		if err != nil {
			panic("Couldn't parse args")
		}
	}
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
	fmt.Printf("Done! chunks: %d\n", totSections)
}
