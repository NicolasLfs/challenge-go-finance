package localenv

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
)

func Load() error {
	directory, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get working directory: %w", err)
	}

	for {
		envPath := filepath.Join(directory, ".env")
		if _, err := os.Stat(envPath); err == nil {
			return godotenv.Load(envPath)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect %s: %w", envPath, err)
		}

		parent := filepath.Dir(directory)
		if parent == directory {
			return nil
		}
		directory = parent
	}
}