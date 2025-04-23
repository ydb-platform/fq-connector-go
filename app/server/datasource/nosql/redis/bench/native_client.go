// bench/main.go
package main

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/redis/go-redis/v9"
	"github.com/shirou/gopsutil/v3/process"
)

type Bench struct {
	mu       sync.Mutex
	start    time.Time
	lastTS   time.Time
	lastCPU  float64
	bytesInt int64
	rows     int64
	interval time.Duration
	proc     *process.Process
	ticker   *time.Ticker
	done     chan struct{}

	sumRateMB   float64
	sumRateRows float64
	sumCPU      float64
	reportCount int
}

func NewBench(interval time.Duration) (*Bench, error) {
	p, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return nil, err
	}

	t, err := p.Times()

	if err != nil {
		return nil, err
	}

	now := time.Now()

	return &Bench{
		start:    now,
		lastTS:   now,
		lastCPU:  t.User + t.System,
		interval: interval,
		proc:     p,
		done:     make(chan struct{}),
	}, nil
}

func (b *Bench) Start() {
	b.ticker = time.NewTicker(b.interval)

	go func() {
		for {
			select {
			case <-b.ticker.C:
				b.report("INTERMEDIATE")
			case <-b.done:
				return
			}
		}
	}()
}

func (b *Bench) Stop() {
	b.ticker.Stop()
	close(b.done)
}

// Add tracks internal byte count and row count
func (b *Bench) Add(internalBytes, rowCount int) {
	b.mu.Lock()
	b.bytesInt += int64(internalBytes)
	b.rows += int64(rowCount)
	b.mu.Unlock()
}

// report logs metrics and tracks for averages
func (b *Bench) report(kind string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(b.start)
	period := now.Sub(b.lastTS).Seconds()

	cpuT, _ := b.proc.Times()
	cpuNow := cpuT.User + cpuT.System
	cpuDelta := cpuNow - b.lastCPU
	cpuUtil := cpuDelta / period * 100.0

	bi := b.bytesInt
	rows := b.rows
	rateMB := float64(bi) / elapsed.Seconds() / 1024 / 1024
	rateRows := float64(rows) / elapsed.Seconds()

	log.Printf("%s RESULT: elapsed=%s, bytes=%s, rate=%.2f MB/s, rows=%d, rowsRate=%.2f rows/s, cpu=%.2f%%",
		kind,
		elapsed.Truncate(time.Millisecond),
		humanize.Bytes(uint64(bi)),
		rateMB,
		rows,
		rateRows,
		cpuUtil,
	)

	if kind == "INTERMEDIATE" {
		b.sumRateMB += rateMB
		b.sumRateRows += rateRows
		b.sumCPU += cpuUtil
		b.reportCount++
	}

	b.lastTS = now
	b.lastCPU = cpuNow
}

// Final logs final and average metrics
func (b *Bench) Final() {
	b.report("FINAL")

	if b.reportCount > 0 {
		avgMB := b.sumRateMB / float64(b.reportCount)
		avgRows := b.sumRateRows / float64(b.reportCount)
		avgCPU := b.sumCPU / float64(b.reportCount)
		log.Printf("AVERAGE: rate=%.2f MB/s, rowsRate=%.2f rows/s, cpu=%.2f%%", avgMB, avgRows, avgCPU)
	}
}

func connectRedis() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		DB:           0,
		PoolSize:     50,
		MinIdleConns: 10,
		DialTimeout:  60 * time.Second, // время на TCP‑connect + AUTH
		ReadTimeout:  60 * time.Second, // 0 => ждать ответ сколько угодно
		WriteTimeout: 60 * time.Second, // 0 => ждать пока пакет отправится
	})
}

//nolint:gocyclo
func scanAll(b *Bench) {
	rdb := connectRedis()
	defer rdb.Close()

	ctx := context.Background()

	var cursor uint64

	for {
		// 1) SCAN batch
		keys, cur, err := rdb.Scan(ctx, cursor, "*", 100000).Result()
		if err != nil {
			log.Fatal(err)
		}

		cursor = cur

		// 2) TYPE pipeline
		pipe := rdb.Pipeline()
		typeCmds := make([]*redis.StatusCmd, len(keys))

		for i, key := range keys {
			typeCmds[i] = pipe.Type(ctx, key)
		}

		if _, err = pipe.Exec(ctx); err != nil {
			log.Printf("TYPE pipeline error: %v", err)
		}

		var strKeys, hashKeys []string

		for i, cmd := range typeCmds {
			t, errRes := cmd.Result()
			if errRes != nil {
				log.Fatalf("cmd result: %v", errRes)
			}

			if t == "string" {
				strKeys = append(strKeys, keys[i])
			} else if t == "hash" {
				hashKeys = append(hashKeys, keys[i])
			}
		}

		// 3) GET pipeline
		if len(strKeys) > 0 {
			pipe = rdb.Pipeline()
			getCmds := make([]*redis.StringCmd, len(strKeys))

			for i, key := range strKeys {
				getCmds[i] = pipe.Get(ctx, key)
			}

			if _, err = pipe.Exec(ctx); err != nil {
				log.Printf("GET pipeline error: %v", err)
			}

			for i, cmd := range getCmds {
				val, errRes := cmd.Result()
				if errRes != nil {
					log.Fatalf("cmd result: %v", errRes)
				}

				b.Add(len(strKeys[i])+len(val), 1)
			}
		}

		// 4) HGETALL pipeline
		if len(hashKeys) > 0 {
			pipe = rdb.Pipeline()
			hgetCmds := make([]*redis.MapStringStringCmd, len(hashKeys))

			for i, key := range hashKeys {
				hgetCmds[i] = pipe.HGetAll(ctx, key)
			}

			if _, err := pipe.Exec(ctx); err != nil {
				log.Printf("HGETALL pipeline error: %v", err)
			}

			for i, cmd := range hgetCmds {
				m, errRes := cmd.Result()
				if errRes != nil {
					log.Fatalf("cmd result: %v", errRes)
				}

				total := 0

				for field, v := range m {
					total += len(field) + len(v)
				}

				b.Add(len(hashKeys[i])+total, 1)
			}
		}

		if cursor == 0 {
			break
		}
	}
}

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	bench, err := NewBench(5 * time.Second)
	if err != nil {
		log.Fatalf("bench init: %v", err)
	}

	bench.Start()

	scanAll(bench)

	bench.Stop()
	bench.Final()
}
