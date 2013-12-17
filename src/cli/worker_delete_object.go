package main

import (
  "stf/worker"
)

func main() {
  controller := worker.NewWorkerControllerFromArgv(
    "DeleteObject",
    "queue_delete_object",
    worker.NewDeleteObjectWorker,
  )
  controller.Start()
}


