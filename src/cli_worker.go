package main

import (
  "log"
  "stf/worker"
)
func main() {
  ctx, err := worker.BootstrapContext()
  if err != nil {
    log.Fatal(err)
  }

  defer ctx.Destroy()

  d := worker.BootstrapWorker(ctx)
  d.Start()
}

