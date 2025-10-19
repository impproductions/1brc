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
	"time"
)

type Stat struct {
	Min   float64
	Tot   float64
	Count int64
	Max   float64
}

var stats map[string]*Stat = make(map[string]*Stat, 0)

func ProcessChunks(f *os.File, chunkSize int64, parallelism int64) int64 {
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
	var tot int64
	var offset int64

	updates := make(chan map[string]*Stat)
	wgRec.Add(1)
	go func() {
		defer wgRec.Done()
		for update := range updates {
			for k, v := range update {
				if val, exists := stats[k]; exists {
					stats[k].Min = min(val.Min, v.Min)
					stats[k].Tot = val.Tot + v.Tot
					stats[k].Count = val.Count + v.Count
					stats[k].Max = max(val.Max, v.Max)
				} else {
					stats[k] = v
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

				if offset > size {
					canc()
				}
			}(offset)
			tot++
			offset += chunkSize
		case <-ctx.Done():
			wgProd.Wait()
			close(updates)
			wgRec.Wait()
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

	cityStart, cityEnd := int64(0), int64(0)
	tempStart, tempEnd := int64(0), int64(0)
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
				cityEnd = p + 1
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
			tempEnd = p + 1
			continue
		}
		if c == '\n' && p <= int64(read)-1 {
			currentCity := string(buf[cityStart:cityEnd])
			// currentTemp := string(buf[tempStart:tempEnd])
			leftOfSemicolon = true
			// parsed, err := strconv.ParseFloat(currentTemp, 64)
			// if err != nil {
			// 	panic(fmt.Sprintf("couldn't parse float: %s;%s", currentCity, currentTemp))
			// }

			parsed := ParseFloat([]byte(buf[tempStart:tempEnd]))

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

			cityStart = p + 1
			cityEnd = p + 1

			lines += 1
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

func ceilTo(n float64, decimals int) float64 {
	factor := math.Pow(10, float64(decimals))
	return math.Ceil(n*factor) / factor
}

func ParseFloat(bytes []byte) float64 {
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
	// cpuProfile, err := os.Create("cpu.prof")
	// if err != nil {
	// 	panic(err)
	// }
	// defer cpuProfile.Close()
	// if err := pprof.StartCPUProfile(cpuProfile); err != nil {
	// 	panic(err)
	// }
	// defer pprof.StopCPUProfile()

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
		fmt.Printf("%s=%.1f/%.1f/%.1f", k, v.Min, v.Tot/float64(v.Count), v.Max)
		if cnt < len(keys)-1 {
			fmt.Printf(", ")
		}
		cnt++
	}
	fmt.Printf("}\n")

	fmt.Printf("Done in %s\n", time.Since(start))
	fmt.Printf("Done! chunks: %d\n", totSections)
}
