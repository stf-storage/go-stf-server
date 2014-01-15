package main

import (
  "github.com/stf-storage/go-stf-server/worker"
)

func main() {
  controller := worker.NewWorkerControllerFromArgv(
    "RepairObject",
    "queue_repair_object",
    worker.NewRepairObjectWorker,
  )
  controller.Start()
}


