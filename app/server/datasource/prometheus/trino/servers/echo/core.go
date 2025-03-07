package main

import (
	"net/http"

	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo/v4"
)

func main() {
	e := echo.New()
	e.Use(echoprometheus.NewMiddleware("echo"))

	e.GET("/metrics", echoprometheus.NewHandler())
	e.GET("/ping", func(c echo.Context) error {
		return c.String(http.StatusOK, "pong")
	})

	e.Logger.Fatal(e.Start(":8081"))
}
