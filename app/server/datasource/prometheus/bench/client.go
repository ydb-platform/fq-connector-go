package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	cfg "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

var bodySize = int64(0)

func main() {
	runtime.GOMAXPROCS(1)
	ctx := context.Background()

	readClient, err := remote.NewReadClient("remote-read-test", &remote.ClientConfig{
		URL:              &config.URL{URL: mustParseURL("http://localhost:9090/api/v1/read")},
		Timeout:          model.Duration(1000 * time.Second),
		ChunkedReadLimit: cfg.DefaultChunkedReadLimit,
	})
	readClient.(*remote.Client).Client = &http.Client{
		Transport: &Transport{Transport: http.DefaultTransport},
	}

	matchers, err := parser.ParseMetricSelector("{__name__!=\"\"}")
	if err != nil {
		log.Fatal(err)
	}

	pbQuery, err := remote.ToQuery(
		int64(model.TimeFromUnixNano(time.Now().Add(-48*time.Hour).UnixNano())),
		int64(model.TimeFromUnixNano(time.Now().UnixNano())),
		matchers,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	vv := 0.0
	metricsCount := int64(0)
	measureCount := 1.0
	measureCountInt := int64(measureCount)
	start := time.Now()
	for range measureCountInt {
		timeseries, err := readClient.Read(ctx, pbQuery, false)
		if err != nil {
			log.Fatal(err)
		}

		var it chunkenc.Iterator
		for timeseries.Next() {
			s := timeseries.At()
			it := s.Iterator(it)

			//l := s.Labels().String()
			for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
				atomic.AddInt64(&metricsCount, 1)
				switch vt {
				case chunkenc.ValFloat:
					ts, v := it.At()
					vv += float64(ts) + v
					//fmt.Printf("%s %g %d\n", l, v, ts)
				case chunkenc.ValHistogram:
					ts, h := it.AtHistogram(nil)
					vv += float64(ts) + h.Sum
					//fmt.Printf("%s %s %d\n", l, h.String(), ts)
				case chunkenc.ValFloatHistogram:
					ts, h := it.AtFloatHistogram(nil)
					vv += float64(ts) + h.Sum
					//fmt.Printf("%s %s %d\n", l, h.String(), ts)
				default:
					panic("unreachable")
				}
			}
			if err := timeseries.Err(); err != nil {
				log.Fatal(err)
			}
		}
	}

	el := time.Since(start).Seconds()
	avgSize := (float64(bodySize) / measureCount) / 1024
	avgTime := el / measureCount

	fmt.Printf("Metrics count: %d\n", metricsCount/measureCountInt)
	fmt.Printf("Size: %.3f KB\n", avgSize)
	fmt.Printf("Avg time: %.5f s\n", avgTime)
	fmt.Printf("Throughput: %.3f KB/s; %.3f MB/s; %.3f GB/s\n", avgSize/avgTime, (avgSize/1024.0)/avgTime, (avgSize/(1024.0*1024.0))/avgTime)
}

func mustParseURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		log.Fatal(err)
	}
	return u
}

type Transport struct {
	Transport http.RoundTripper
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := t.Transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	body, err := t.cloneBody(res.Body)
	if err != nil {
		return nil, err
	}
	atomic.AddInt64(&bodySize, int64(len(body)))

	res.Body = io.NopCloser(bytes.NewBuffer(body))

	return res, nil
}

func (t *Transport) cloneBody(body io.ReadCloser) ([]byte, error) {
	if body == nil {
		return nil, nil
	}

	buf := new(bytes.Buffer)

	_, err := buf.ReadFrom(body)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
