// +build q4m

package stftest

import(
  "fmt"
  "os"
)

func (self *TestEnv) startQueue() {
  config := self.MysqlConfig
  self.QueueConfig = &QueueConfig {
    config.Dbtype,
    config.Username,
    config.Password,
    config.ConnectString,
    "test_queue",
  }

  self.createQueue()
}

func (self *TestEnv) createQueue() {
  t := self.Test

  db, err := ConnectDB(self.MysqlConfig)
  if err != nil {
    t.Errorf("Failed to connect to database: %s", err)
    t.FailNow()
  }

  _, err = db.Exec("CREATE DATABASE test_queue")
  if err != nil {
    t.Errorf("Failed to create database test_queue: %s", err)
    t.FailNow()
  }
}

func (self *TestEnv) writeQueueConfig(tempfile *os.File) {
  tempfile.WriteString(fmt.Sprintf(
`
[QueueDB "1"]
Username=%s
ConnectString=%s
Dbname=%s
`,
    self.QueueConfig.Username,
    self.QueueConfig.ConnectString,
    self.QueueConfig.Dbname,
  ))
}
