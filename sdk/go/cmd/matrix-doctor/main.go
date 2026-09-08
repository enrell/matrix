// Command matrix-doctor diagnoses the Matrix operator environment
// (no secrets printed). Usage: matrix-doctor [--binary <path>]
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	mx "matrix-component-go"
)

func main() {
	binary := flag.String("binary", "", "matrix-managed binary path")
	flag.Parse()
	rep := mx.Doctor(*binary)
	raw, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(raw))
	if !rep.CLIShapeOK {
		os.Exit(1)
	}
}
