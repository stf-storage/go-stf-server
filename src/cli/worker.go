package main

import (
  "stf/worker"
)
func main() {
  drone := worker.NewDroneFromArgv()
  drone.Start()
}

