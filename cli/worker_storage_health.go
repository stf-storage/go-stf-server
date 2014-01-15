package main

import (
  "github.com/stf-storage/go-stf-server/worker"
)

func main() {
  controller := worker.NewWorkerControllerFromArgv(
    "StorageHealth",
    "",
    worker.NewStorageHealthWorker,
  )
  controller.Start()
}


