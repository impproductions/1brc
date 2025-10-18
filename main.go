package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"sync/atomic"
)

var stats map[string]float32 = make(map[string]float32, 0)
var lc atomic.Int64

func ProcessChunks(r io.ReaderAt, chunkSize int64, parallelism int64) int {
	tot := 0
	permit := struct{}{}
	var offset int64
	semaphoreChan := make(chan struct{}, parallelism)
	done := make(chan struct{})

	for {
		select {
		case semaphoreChan <- permit:
			go func(o int64) {
				_, err := ProcessChunk(r, o, chunkSize)
				if err != nil && err != io.EOF {
					panic(err)
				}
				<-semaphoreChan
				if err == io.EOF {
					done <- struct{}{}
				}
			}(offset)
			offset += chunkSize
		case <-done:
			// close(semaphoreChan)
			return tot
		}
	}
}

func ProcessChunk(r io.ReaderAt, from, bytes int64) (int, error) {
	extra := int64(64)
	buf := make([]byte, bytes+extra)

	read, err := r.ReadAt(buf, from)
	if err != nil && err != io.EOF {
		panic(err)
	}
	fmt.Printf("Chunk %d[%d] %s-%s\n", from, from+bytes, string(buf[0:5]), string(buf[read-6:read-1]))

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
		if c == '\n' && p <= int64(read)-1 {
			leftOfSemicolon = true
			// cur := stats[string(currentCity)]
			parsed, err := strconv.ParseFloat(string(currentTemp), 32)
			if err != nil {
				panic(fmt.Sprintf("couldn't parse float: %s;%s", currentCity, currentTemp))
			}
			fmt.Printf("Storing %s -> %f (eof: %v)\n", string(currentCity), parsed, err)
			// stats[string(currentCity)] = cur + float32(parsed)
			currentCity = []byte{}
			currentTemp = []byte{}
			lines += 1
			continue
		}
		if c == ';' {
			leftOfSemicolon = false
			continue
		}

		if leftOfSemicolon {
			currentCity = append(currentCity, c)
		} else {
			currentTemp = append(currentTemp, c)
		}
	}

	lc.Add(int64(lines))

	return read, err
}

func main() {
	const chunkSize = 256 // 4194304 * 64
	const parallelism = 1
	const filePath = "assets/measurements_med.txt"

	f, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	totSections := ProcessChunks(f, chunkSize, parallelism)

	fmt.Printf("Done! %d, lines: %d\n", totSections, lc.Load())
}
