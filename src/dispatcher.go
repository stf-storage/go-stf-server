package main

import (
  "flag"
  "log"
  "os"
  "path"
  "stf/context"
  "stf/dispatcher"
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
  ctx, err := context.Bootstrap()
  if err != nil {
    log.Fatal(err)
  }

  defer ctx.Destroy()

  d, err := dispatcher.Bootstrap(ctx)
  d.Start()
}