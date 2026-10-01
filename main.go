package main

import (
	"challenge/app"

	"go.uber.org/fx"
)

func main() {
	fx.New(app.Module).Run()
}
