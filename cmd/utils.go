package cmd

import (
	"fmt"
	"os"
)

func exitOnError(err error) {
	if err != nil {
		fmt.Printf("ERROR: %s\n", err)
		os.Exit(-1)
	}
}

func warnOnError(err error) {
	if err != nil {
		fmt.Printf("WARN: %s\n", err)
	}
}
