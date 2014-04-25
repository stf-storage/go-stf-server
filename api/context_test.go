package api

import (
	"github.com/stf-storage/go-stf-server/config"
	"log"
)

func ExampleScopedContext() {
	config, err := config.BootstrapConfig()
	if err != nil {
		log.Fatalf("Failed to bootstrap config: %s", err)
	}

	for {
		ctx := NewContext(config)
		rollback, err := ctx.TxnBegin()
		if err != nil {
			log.Fatalf("Failed to start transaction: %s", err)
		}
		defer rollback()

		// Do stuff...
		ctx.TxnCommit()
	}
}
