package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"
)

type Stat struct {
	Count int32
	Min   float64
	Tot   float64
	Max   float64
}

const (
	CITY        = 0
	TEMPERATURE = 1
)

const CHUNK_SIZE = 16777216
const PARALLELISM = 8
const FILE_PATH = "assets/measurements_max.txt"
const STATIONS = 10000

var stats map[string]*Stat = make(map[string]*Stat, STATIONS)

func ProcessChunks(f *os.File, chunkSize int, parallelism int64) {
	ctx, canc := context.WithCancel(context.Background())
	defer canc()

	info, err := f.Stat()
	if err != nil {
		panic("couldn't retreive file stats")
	}

	permit := struct{}{}
	semaphore := make(chan struct{}, parallelism)

	var wgProd sync.WaitGroup
	var wgRec sync.WaitGroup

	var size int64 = info.Size()
	var offset int64
	updates := make(chan map[string]*Stat)

	wgRec.Add(1)
	go func() {
		defer wgRec.Done()
		for statsUpdate := range updates {
			for k, updatedVal := range statsUpdate {
				if val, exists := stats[k]; exists {
					val.Min = min(val.Min, updatedVal.Min)
					val.Tot = val.Tot + updatedVal.Tot
					val.Count = val.Count + updatedVal.Count
					val.Max = max(val.Max, updatedVal.Max)
				} else {
					stats[k] = updatedVal
				}
			}
		}
	}()

	for {
		select {
		case semaphore <- permit:
			if offset > size {
				<-semaphore
				canc()
				continue
			}
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

			}(offset)
			offset += int64(chunkSize)
		case <-ctx.Done():
			wgProd.Wait()
			close(updates)
			wgRec.Wait()
			return
		}
	}
}

func ProcessChunk(r io.ReaderAt, from int64, bytes int) (map[string]*Stat, error) {
	extra := 64
	overshotLength := bytes + extra
	buf := make([]byte, overshotLength)

	read, err := r.ReadAt(buf, from)
	if err != nil {
		if err != io.EOF {
			panic(err)
		} else if read == 0 {
			return nil, err
		}
	}

	start := 0
	for ; buf[start] != '\n' && from > 0; start++ {
	}
	if from == 0 {
		start = -1
	}

	limit := min(read, bytes)
	localStats := make(map[string]*Stat, STATIONS)
	starts := make([]int, 2)
	ends := make([]int, 2)
	// city cursors
	starts[CITY] = start + 1
	ends[CITY] = starts[CITY]
	// temperature cursors
	starts[TEMPERATURE] = 0
	ends[TEMPERATURE] = 0
	mode := CITY

	prev := byte(0)
	for i := starts[CITY]; i < overshotLength; i++ {
		c := buf[i]
		prev = c

		if i > limit && prev != '\n' {
			break
		}

		switch {
		case c != '\n' && c != ';':
			ends[mode]++
		case c == '\n':
			unsafe_currentCityString := unsafe.String(&buf[starts[CITY]], ends[CITY]-starts[CITY]) // string() just to access the map takes up a huge chunk of time
			currentTemp := parseFloat(buf[starts[TEMPERATURE]:ends[TEMPERATURE]])

			val, found := localStats[unsafe_currentCityString]
			if !found {
				val = &Stat{
					Min: currentTemp,
					Max: currentTemp,
				}
				localStats[string(buf[starts[CITY]:ends[CITY]])] = val
			}

			val.Min = min(val.Min, currentTemp)
			val.Tot = val.Tot + currentTemp
			val.Count++
			val.Max = max(val.Max, currentTemp)

			mode = CITY
			starts[CITY] = i + 1
			ends[CITY] = starts[CITY]
		default:
			mode = TEMPERATURE
			starts[TEMPERATURE] = i + 1
			ends[TEMPERATURE] = starts[TEMPERATURE]
		}
	}

	return localStats, err
}

func parseFloat(bytes []byte) float64 {
	negative := bytes[0] == '-'
	number := int32(0)
	l := len(bytes)

	start := 0
	if negative {
		start = 1
	}

	// we can exploit the fact that temps are guaranteed to have 1 decimal digit to skip the .
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
	start := time.Now()
	f, err := os.Open(FILE_PATH)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	ProcessChunks(f, CHUNK_SIZE, PARALLELISM)

	var builder strings.Builder
	builder.WriteString("{")
	keys := make([]string, len(stats))
	cnt := 0
	for k := range stats {
		keys[cnt] = k
		cnt++
	}
	sort.Strings(keys)
	for i, k := range keys {
		v := stats[k]
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", k, v.Min, v.Tot/float64(v.Count), v.Max))
	}
	builder.WriteString("}\n")
	os.WriteFile("result.txt", []byte(builder.String()), 0644)

	fmt.Printf("Done in %s\n", time.Since(start))

	os.Exit(0)
}
