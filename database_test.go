package stf

import (
	"github.com/stf-storage/go-stf-server/config"
	"log"
)

func ExampleDatabase() {
	config := &config.DatabaseConfig{
		"mysql",
		"root",
		"password",
		"tcp(127.0.0.1:3306)",
		"stf",
	}

	db, err := ConnectDB(config)
	if err != nil {
		log.Fatalf("Failed to connecto database: %s", err)
	}

	db.QueryRow("SELECT ...")
}
