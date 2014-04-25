package main

import (
	"flag"
	"github.com/stf-storage/go-stf-server/config"
	"github.com/stf-storage/go-stf-server/drone"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func main() {
	var configFile string

	pwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("Could not determine current working directory")
	}

	defaultConfig := path.Join(pwd, "etc", "config.gcfg")

	flag.StringVar(
		&configFile,
		"config",
		defaultConfig,
		"Path to config file",
	)
	flag.Parse()

	os.Setenv("STF_CONFIG", configFile)

	config, err := config.BootstrapConfig()
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
	os.Setenv("PATH", strings.Join([]string{p, dir}, ":"))

	drone.NewDrone(config).Run()
}
