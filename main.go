package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime/pprof"
	"sort"
	"sync"
	"time"
	"unsafe"
)

type Stat struct {
	Min   float64
	Tot   float64
	Count int64
	Max   float64
}

var stats map[string]*Stat = make(map[string]*Stat, 500)

func ProcessChunks(f *os.File, chunkSize int64, parallelism int64) {
	ctx, canc := context.WithCancel(context.Background())
	defer canc()

	info, err := f.Stat()
	if err != nil {
		panic("couldn't retreive file stats")
	}

	var wgProd sync.WaitGroup
	var wgRec sync.WaitGroup

	permit := struct{}{}
	semaphore := make(chan struct{}, parallelism)

	var size int64 = info.Size()
	var offset int64

	updates := make(chan map[string]*Stat)
	wgRec.Add(1)
	go func() {
		defer wgRec.Done()
		for statsUpdate := range updates {
			for k, updatedVal := range statsUpdate {
				if val, exists := stats[k]; exists {
					stats[k].Min = min(val.Min, updatedVal.Min)
					stats[k].Tot = val.Tot + updatedVal.Tot
					stats[k].Count = val.Count + updatedVal.Count
					stats[k].Max = max(val.Max, updatedVal.Max)
				} else {
					stats[k] = updatedVal
				}
			}
		}
	}()

	for {
		select {
		case semaphore <- permit:
			wgProd.Add(1)
			go func(o int64) {
				defer wgProd.Done()
				computedStats, err := ProcessChunk(f, o, chunkSize)
				if err != nil && err != io.EOF {
					panic(err)
				}
				if err == io.EOF {
					canc()
				}
				<-semaphore
				updates <- computedStats

				if o > size {
					canc()
				}
			}(offset)
			offset += chunkSize
		case <-ctx.Done():
			wgProd.Wait()
			close(updates)
			wgRec.Wait()
			return
		}
	}
}

func ProcessChunk(r io.ReaderAt, from, bytes int64) (map[string]*Stat, error) {
	extra := int64(64)
	overshotLength := bytes + extra
	buf := make([]byte, overshotLength)

	read, err := r.ReadAt(buf, from)
	if err != nil && err != io.EOF {
		panic(err)
	}

	if read == 0 && err == io.EOF {
		return nil, err
	}

	limit := min(int64(read), bytes)

	localStats := make(map[string]*Stat, 500)

	cityStart, cityEnd := int64(0), int64(0)
	tempStart, tempEnd := int64(0), int64(0)
	leftOfSemicolon := true
	isFirstLine := true
	if from == 0 {
		isFirstLine = false
	}
	for p := range overshotLength {
		c := buf[p]
		if isFirstLine {
			if c == '\n' {
				isFirstLine = false
				cityStart = p + 1
				cityEnd = p + 1
			}
			continue
		}
		if p > limit {
			if buf[p-1] == '\n' {
				// we've overshot and are done processing the last line of the chunk
				break
			}
		}
		if c == ';' {
			leftOfSemicolon = false
			tempStart = p + 1
			tempEnd = p + 1
			continue
		}
		if c == '\n' && p <= int64(read)-1 {
			unsafeCurrentCityString := unsafeBytesAsString(buf[cityStart:cityEnd])
			currentTemp := parseFloat(buf[tempStart:tempEnd])
			leftOfSemicolon = true

			val, found := localStats[unsafeCurrentCityString]
			if !found {
				val = &Stat{
					Min: currentTemp,
					Max: currentTemp,
				}
				currentCitySafe := string(buf[cityStart:cityEnd])
				localStats[currentCitySafe] = val
			}

			val.Min = min(val.Min, currentTemp)
			val.Tot = val.Tot + currentTemp
			val.Count++
			val.Max = max(val.Max, currentTemp)

			cityStart = p + 1
			cityEnd = p + 1

			continue
		}

		if leftOfSemicolon {
			cityEnd++
		} else {
			tempEnd++
		}
	}

	return localStats, err
}

// func ceilTo(n float64, decimals int) float64 {
// 	factor := math.Pow(10, float64(decimals))
// 	return math.Ceil(n*factor) / factor
// }

func unsafeBytesAsString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

func parseFloat(bytes []byte) float64 {
	negative := bytes[0] == '-'
	number := int32(0)
	l := len(bytes)

	start := 0
	if negative {
		start = 1
	}
	for i := start; i < l-2; i++ {
		number = number*10 + int32(bytes[i]-'0')
	}
	number = number*10 + int32(bytes[l-1]-'0')

	if negative {
		return -float64(number) * 0.1
	}
	return float64(number) * 0.1
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

	const chunkSize = int64(16777216)
	const parallelism = 8
	const filePath = "assets/measurements_max.txt"

	start := time.Now()
	f, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	ProcessChunks(f, chunkSize, parallelism)

	fmt.Printf("{")
	keys := make([]string, len(stats))
	cnt := 0
	for k := range stats {
		keys[cnt] = k
		cnt++
	}
	sort.Strings(keys)
	for i, k := range keys {
		v := stats[k]
		fmt.Printf("%s=%.1f/%.1f/%.1f", k, v.Min, v.Tot/float64(v.Count), v.Max)
		if i < len(keys)-1 {
			fmt.Printf(", ")
		}
	}
	fmt.Printf("}\n")

	fmt.Printf("Done in %s\n", time.Since(start))
}
