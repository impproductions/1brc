# 1 Billion Row Challenge

My take on the [1 Billion Row Challenge](https://github.com/gunnarmorling/1brc) (1BRC), a programming challenge created to explore Java performance optimization. The task is to process a text file containing 1 billion rows of temperature measurements and calculate min, mean, and max temperatures per weather station as quickly as possible.

## Background

I recently started a job at a company where Go is the main language. It's been a while since my last Go project, and I wanted to refresh my memory, so I decided to finally take on the challenge after months of thinking about trying it.

## Results

On my 12-core M4 Pro:

### With Limited Parallelism (8)

```bash
$ go build && time ./brc
Done in 3.732947125s
./brc  25.36s user 1.78s system 713% cpu 3.804 total
```

I'm too lazy to spin up an 8-core VM just to run this, so I've decided to limit parallelism to 8 to (kind of) match the official benchmark environment.

### With Full Parallelism

```bash
$ go build && time ./brc
Done in 3.232516625s
./brc  29.21s user 2.20s system 921% cpu 3.409 total
```

## Features

- Custom float parser exploiting the single decimal digit guarantee
- `unsafe.String` for zero-copy map lookups (with proper allocation when storing keys)
- Single-pass byte-level parsing without tokenization
- Chunked parallel file processing
- Semaphore-based bounded parallelism
- Local per-chunk aggregation to minimize contention
- Pre-sized maps (10k capacity, the maximum amount of possible stations) to avoid rehashing
- Dedicated aggregator goroutine to avoid lock contention
- Buffer overshoot for handling line boundaries at chunk edges
- Integer-based arithmetic (parse as int32, then convert to float64) to avoid floating point operations during parsing

## Journey

### First iteration (baseline implementation, 13-17s)

- Basic chunked file processing with semaphore-based parallelism
- Initial Stat struct (min/tot/count/max) with context-based coordination
- File size detection and local per-chunk aggregation

### Second iteration (stop growing slices & fancy float parsing -> 7-8s)

- Profiled that an inordinate chunk of time was spent parsing floats and growing slices (duh)
- Replaced byte slice accumulation with buffer pointers (start/end indices)
- Minimized string conversions during parsing
- Custom float parser exploiting single decimal digit guarantee (int32 arithmetic)

### Third iteration (strings are byte buffers, don't be a chicken -> 4-5s)

- Introduced `unsafe.String` for zero-allocation map lookups

### Fourth iteration (who said code can't be more readable AND faster?! -> 3-4s)

- Cleanup & refactoring
- Mode-based parsing refactor using state constants (CITY/TEMPERATURE)
- Changed Count to int32 for better memory layout
- Switched output to strings.Builder
- Improved loop termination logic

## Possible Improvements

Maybe some shenanigans with a hand-rolled map? But I'm not convinced the gain over just using `unsafe.String` would justify the complexity.

## Conclusions

This was a **super** fun challenge that gave me the opportunity to explore so many different areas of optimization that I don't necessarily get close to in my day to day work, from concurrency patterns and lock-free coordination, to memory management with unsafe operations, custom parsing algorithms, and data structure tuning. And it made me realize how even just a single, apparently harmless operation or check can make the difference when you're processing it 300 million times per second!