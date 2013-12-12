package stf

import (
  "crypto/sha1"
  "database/sql"
  "errors"
  "fmt"
  "io"
  "os/user"
  "strconv"
  "time"
)

const VERSION = "0.0.1"

func GenerateRandomId(hint string, length int) string {
  h := sha1.New()
  io.WriteString(h, hint)
  io.WriteString(h, strconv.FormatInt(time.Now().UTC().UnixNano(), 10))
  return (fmt.Sprintf("%x", h.Sum(nil)))[0:length]
}

func ConnectDB(dbg DebugWriter, config *DatabaseConfig) (*sql.DB, error) {
  if config.Dbtype == "" {
    config.Dbtype = "mysql"
  }

  if config.ConnectString == "" {
    switch config.Dbtype {
    case "mysql":
      config.ConnectString = "tcp(127.0.0.1:3306)"
    default:
      return nil, errors.New(
        fmt.Sprintf(
          "No database connect string provided, and can't assign a default value for dbtype '%s'",
          config.Dbtype,
        ),
      )
    }
  }

  if config.Username == "" {
    u, err := user.Current()
    if err == nil {
      config.Username = u.Username
    } else {
      config.Username = "root"
    }
  }

  if config.Dbname == "" {
    config.Dbname = "stf"
  }

  dsn := fmt.Sprintf(
    "%s:%s@%s/%s?parseTime=true",
    config.Username,
    config.Password,
    config.ConnectString,
    config.Dbname,
  )

  dbg.Debugf("Connecting to dsn: %s", dsn)

  db, err := sql.Open(config.Dbtype, dsn)
  if err != nil {
    return nil, errors.New(
      fmt.Sprintf("Failed to connect to database: %s", err),
    )
  }

  return db, nil
}


