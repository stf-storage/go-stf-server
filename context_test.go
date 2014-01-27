package stf

import (
  "log"
)

func ExampleScopedContext_NewScope() {
  config, err := BootstrapConfig()
  if err != nil {
    log.Fatalf("Failed to bootstrap config: %s", err)
  }

  for {
    ctx := NewContext(config)
    release, err := ctx.TxnBegin()
    if err != nil {
      log.Fatalf("Failed to start transaction: %s", err)
    }
    defer release()

    // Do stuff...
    ctx.TxnCommit()
  }
}
