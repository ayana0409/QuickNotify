package main

import (
	"flag"
	"fmt"
	"os"

	"quicknotify/internal/app"
)

func main() {
	// Allow specifying the .env file path via CLI flag; defaults to ".env" in the working directory
	envPath := flag.String("env", ".env", "Path to the .env configuration file")
	flag.Parse()

	// Initialise and wire all application dependencies from the .env file
	application, err := app.New(*envPath)
	if err != nil {
		fmt.Printf("FATAL: failed to initialise application: %v\n", err)
		os.Exit(1)
	}

	// Run blocks until a termination signal is received, then shuts down cleanly
	application.Run()
}
