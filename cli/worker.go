package main

import (
  "log"
  "os"
  "path/filepath"
  "github.com/stf-storage/go-stf-server/worker"
  "strings"
)
func main() {
  // Get our path, and add this to PATH, so other binaries
  // can safely be executed
  dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
  if err != nil {
    log.Fatalf("Failed to find our directory name?!: %s", err)
  }

  path := os.Getenv("PATH")

  os.Setenv("PATH", strings.Join([]string { path, dir }, string(os.PathListSeparator) ))

  drone := worker.NewDroneFromArgv()
  drone.Start()
}

