package main

import (
  "github.com/stf-storage/go-stf-server/worker"
)

func main() {
  controller := worker.NewWorkerControllerFromArgv(
    "DeleteObject",
    "queue_delete_object",
    worker.NewDeleteObjectWorker,
  )
  controller.Start()
}


