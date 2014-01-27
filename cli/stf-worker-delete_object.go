package main

import (
  "github.com/stf-storage/go-stf-server/worker"
)

func main() {
  worker.NewDeleteObjectWorker().Run()
}


