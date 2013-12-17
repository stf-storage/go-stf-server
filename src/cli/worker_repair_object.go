package main

import (
  "stf/worker"
)

func main() {
  controller := worker.NewWorkerControllerFromArgv(
    "RepairObject",
    "queue_repair_object",
    worker.NewRepairObjectWorker,
  )
  controller.Start()
}


