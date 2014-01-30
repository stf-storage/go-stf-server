package main

import (
  "flag"
  "log"
  "os"
  "path"
  "github.com/stf-storage/go-stf-server"
)

func main() {
  var configFile string

  pwd, err := os.Getwd()
  if err != nil {
    log.Fatalf("Could not determine current working directory")
  }

  var dispatcherId uint64
  defaultConfig := path.Join(pwd, "etc", "config.gcfg")

  flag.Uint64Var(
    &dispatcherId,
    "id",
    0,
    "Dispatcher ID, overrides config file settings",
  )
  flag.StringVar(
    &configFile,
    "config",
    defaultConfig,
    "Path to config file",
  )
  flag.Parse()

  os.Setenv("STF_CONFIG", configFile)
  config, err := stf.BootstrapConfig()
  if err != nil {
    log.Fatal(err)
  }

  if dispatcherId > 0 {
    config.Dispatcher.ServerId = dispatcherId
  }

  d := stf.NewDispatcher(config)
  d.Start()
}