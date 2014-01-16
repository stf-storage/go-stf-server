package main

import (
  "github.com/stf-storage/go-stf-server/worker"
)

func main() {
  ctx := worker.NewWorkerContext()
  controller := worker.NewWorkerControllerFromArgv(
    ctx,
    "DeleteObject",
    "queue_delete_object",
    worker.NewDeleteObjectWorker,
  )
  controller.Start()
}


