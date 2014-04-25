package main

import (
	"github.com/stf-storage/go-stf-server/worker"
)

func main() {
	ctx := worker.NewWorkerContext()
	controller := worker.NewWorkerControllerFromArgv(
		ctx,
		"StorageHealth",
		"",
		worker.NewStorageHealthWorker,
	)
	controller.Start()
}
