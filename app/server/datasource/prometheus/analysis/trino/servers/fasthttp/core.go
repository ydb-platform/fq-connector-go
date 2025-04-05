package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	fasthttprouter "github.com/fasthttp/router"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	metrics "github.com/slok/go-http-metrics/metrics/prometheus"
	"github.com/slok/go-http-metrics/middleware"
	fasthttpmiddleware "github.com/slok/go-http-metrics/middleware/fasthttp"
	"github.com/valyala/fasthttp"
)

const (
	srvAddr     = ":8082"
	metricsAddr = ":8081"
)

func main() {
	mdlw := middleware.New(middleware.Config{
		Recorder: metrics.NewRecorder(metrics.Config{}),
	})

	r := fasthttprouter.New()

	r.GET("/ping", func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("pong")
	})

	fasthttpHandler := fasthttpmiddleware.Handler("", mdlw, r.Handler)

	go func() {
		log.Printf("server listening at %s", srvAddr)

		if err := fasthttp.ListenAndServe(srvAddr, fasthttpHandler); err != nil {
			log.Panicf("error while serving: %s", err)
		}
	}()

	go func() {
		log.Printf("metrics listening at %s", metricsAddr)

		if err := http.ListenAndServe(metricsAddr, promhttp.Handler()); err != nil {
			log.Panicf("error while serving metrics: %s", err)
		}
	}()

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGTERM, syscall.SIGINT)
	<-sigC
}
