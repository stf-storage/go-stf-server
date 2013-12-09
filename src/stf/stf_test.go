package stf

import (
  "log"
  "net/http/httptest"
  "testing"
)

func TestBasic(t *testing.T) {
  ctx, err := BootstrapContext()
  if err != nil {
    log.Fatal(err)
  }

  defer ctx.Destroy()

  d, err := BootstrapDispatcher(ctx)
  dts := httptest.NewServer(d)
  defer dts.Close()
}