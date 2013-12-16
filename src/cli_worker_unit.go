package main

import (
  "stf/worker"
)
func main() {
  controller := worker.NewWorkerControllerFromArgv()
  controller.Start()
}


