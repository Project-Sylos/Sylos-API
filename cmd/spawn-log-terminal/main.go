package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
)

func main() {
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s <log-address>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s 127.0.0.1:8081\n", os.Args[0])
		os.Exit(1)
	}

	logAddress := flag.Arg(0)

	if err := logservice.RunListener(logAddress); err != nil {
		fmt.Fprintf(os.Stderr, "Error running log listener: %v\n", err)
		os.Exit(1)
	}
}
