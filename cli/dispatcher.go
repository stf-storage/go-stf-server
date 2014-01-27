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

  defaultConfig := path.Join(pwd, "etc", "config.gcfg")

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

  d := stf.NewDispatcher(config)
  d.Start()
}