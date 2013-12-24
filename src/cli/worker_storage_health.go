package main

import (
  "stf/worker"
)

func main() {
  controller := worker.NewWorkerControllerFromArgv(
    "StorageHealth",
    "",
    worker.NewStorageHealthWorker,
  )
  controller.Start()
}


