package main

import (
  "flag"
  "log"
  "os"
  "github.com/stf-storage/go-stf-server"
)

func main() {
  var listen string
  var root string

  pwd, err := os.Getwd()
  if err != nil {
    log.Fatalf("Could not determine current working directory")
  }

  flag.StringVar(&listen, "listen", ":9000", "Interface/port to listen to")
  flag.StringVar(&root, "root", pwd, "Path to store/fetch files to/from")
  flag.Parse()

  ss := stf.NewStorageServer(listen, root)
  ss.Start()
}
