package main

import (
	"flag"
	"github.com/stf-storage/go-stf-server/config"
	"github.com/stf-storage/go-stf-server/dispatcher"
	"log"
	"os"
	"path"
)

func main() {
	var configFile string
	var dispatcherId uint64
	var listen string

	pwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("Could not determine current working directory")
	}

	defaultConfig := path.Join(pwd, "etc", "config.gcfg")

	flag.Uint64Var(
		&dispatcherId,
		"id",
		0,
		"Dispatcher ID, overrides config file settings",
	)
	flag.StringVar(
		&listen,
		"listen",
		"",
		"host:port to listen on",
	)
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
		log.Fatal(err)
	}

	if dispatcherId > 0 {
		config.Dispatcher.ServerId = dispatcherId
	}
	if listen != "" {
		config.Dispatcher.Listen = listen
	}

	d := dispatcher.New(config)
	d.Start()
}
