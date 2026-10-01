package repository

import "challenge/internal/localenv"

func init() {
	if err := localenv.Load(); err != nil {
		panic(err)
	}
}
