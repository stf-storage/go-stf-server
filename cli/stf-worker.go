package main

import (
  "log"
  "os"
  "path/filepath"
"strings"
  "github.com/stf-storage/go-stf-server"
  "github.com/stf-storage/go-stf-server/drone"
)

func main() {
  config, err := stf.BootstrapConfig()
  if err != nil {
    log.Fatalf("Could not load config: %s", err)
  }

  // Get our path, and add this to PATH, so other binaries
  // can safely be executed
  dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
  if err != nil {
    log.Fatalf("Failed to find our directory name?!: %s", err)
  }

  p := os.Getenv("PATH")
  os.Setenv("PATH", strings.Join([]string{ p, dir }, ":"))

  drone.NewDrone(config).Run()
}
