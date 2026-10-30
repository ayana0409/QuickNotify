package main

import (
	"flag"
	"fmt"
	"os"

	"quicknotify/internal/app"
)

func main() {
	// Allow overriding the config file path via a CLI flag
	configPath := flag.String("config", "configs/config.yaml", "Path to the application configuration YAML file")
	flag.Parse()

	// Initialise and wire all application dependencies
	application, err := app.New(*configPath)
	if err != nil {
		fmt.Printf("FATAL: failed to initialise application: %v\n", err)
		os.Exit(1)
	}

	// Run blocks until a termination signal is received, then shuts down cleanly
	application.Run()
}
