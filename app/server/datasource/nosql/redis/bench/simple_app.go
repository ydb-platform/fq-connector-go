// bench/main.go
package main

import (
	"context"
	"fmt"
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
	bytesArr int64
	rows     int64
	interval time.Duration
	proc     *process.Process
	ticker   *time.Ticker
	done     chan struct{}
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

func (b *Bench) Add(internal, arrow, rows int) {
	b.mu.Lock()
	b.bytesInt += int64(internal)
	b.bytesArr += int64(arrow)
	b.rows += int64(rows)
	b.mu.Unlock()
}

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

	bi, ba, rows := b.bytesInt, b.bytesArr, b.rows
	rateInt := float64(bi) / elapsed.Seconds()
	rateArr := float64(ba) / elapsed.Seconds()
	rateRows := float64(rows) / elapsed.Seconds()

	log.Printf("%s RESULT: elapsed time: %s, bytes internal = %s (%.2f MB/s), bytes arrow = %s (%.2f MB/s), rows = %d (%.2f rows/s), cpu = %.2f%%",
		kind,
		elapsed.Truncate(time.Millisecond),
		humanize.Bytes(uint64(bi)), rateInt/1024/1024,
		humanize.Bytes(uint64(ba)), rateArr/1024/1024,
		rows, rateRows,
		cpuUtil,
	)

	b.lastTS = now
	b.lastCPU = cpuNow
}

func (b *Bench) Final() {
	b.report("FINAL")
}

func connectRedis() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		DB:           0,
		PoolSize:     50,
		MinIdleConns: 10,
	})
}

func scanAll(b *Bench) {
	rdb := connectRedis()
	defer rdb.Close()

	ctx := context.Background()
	var cursor uint64

	for {
		// 1) Сканируем пачкой 100k ключей
		keys, cur, err := rdb.Scan(ctx, cursor, "*", 100000).Result()
		if err != nil {
			log.Fatal(err)
		}
		cursor = cur

		// 2) Pipeline для TYPE
		pipe := rdb.Pipeline()
		typeCmds := make([]*redis.StatusCmd, len(keys))
		for i, key := range keys {
			typeCmds[i] = pipe.Type(ctx, key)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			log.Printf("TYPE pipeline error: %v", err)
		}

		// 3) Разделяем ключи по типу
		var strKeys, hashKeys []string
		for i, cmd := range typeCmds {
			t, err := cmd.Result()
			if err != nil {
				continue
			}
			switch t {
			case "string":
				strKeys = append(strKeys, keys[i])
			case "hash":
				hashKeys = append(hashKeys, keys[i])
			}
		}

		// 4) Pipeline GET для строк
		if len(strKeys) > 0 {
			pipe = rdb.Pipeline()
			getCmds := make([]*redis.StringCmd, len(strKeys))
			for i, key := range strKeys {
				getCmds[i] = pipe.Get(ctx, key)
			}
			if _, err := pipe.Exec(ctx); err != nil {
				log.Printf("GET pipeline error: %v", err)
			}
			for _, cmd := range getCmds {
				val, err := cmd.Result()
				if err != nil {
					continue
				}
				n := len(val)
				b.Add(n, n, 1)
			}
		}

		// 5) Pipeline HGETALL для хешей
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
				m, err := cmd.Result()
				if err != nil {
					continue
				}
				total := 0
				for _, v := range m {
					total += len(v)
				}
				b.Add(total, total, 1)
				fmt.Printf("[HASH] key=%s, fields=%d, bytes=%d\n", hashKeys[i], len(m), total)
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
