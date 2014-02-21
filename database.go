package stf

import (
  "database/sql"
  "errors"
  "fmt"
  "github.com/stf-storage/go-stf-server/config"
)

type DB struct { sql.DB }

func ConnectDB(config *config.DatabaseConfig) (*DB, error) {
  dsn, err := config.Dsn()
  if err != nil {
    return nil, err
  }

  Debugf("Connecting to database %s", dsn)
  db, err := sql.Open(config.Dbtype, dsn)
  if err != nil {
    return nil, errors.New(
      fmt.Sprintf("Failed to connect to database: %s", err),
    )
  }

  return &DB { *db }, nil
}

