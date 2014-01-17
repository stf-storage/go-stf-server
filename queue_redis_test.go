// +build redis

package stf

import (
  "fmt"
  "os"
  "os/exec"
  _ "github.com/vmihailenco/redis/v2"
)

func (self *TestEnv) startQueue() {
  self.QueueConfig = &QueueConfig{
    Addr: "127.0.0.1:6379",
  }

  path, err := exec.LookPath("redis-server")
  if err != nil {
    self.Test.Errorf("Failed to find redis-server: %s", err)
    self.Test.FailNow()
  }
  cmd := exec.Command(path)

  go func() {
    err := cmd.Start()
    if err != nil {
      self.Test.Errorf("Failed to run redis-server: %s", err)
    }
  }()

  self.AddGuard(func() {
    cmd.Process.Kill()
  })
}

func (self *TestEnv) writeQueueConfig(tempfile *os.File) {
  tempfile.WriteString(fmt.Sprintf(
`
[QueueDB "1"]
Addr=%s
`,
    self.QueueConfig.Addr,
  ))
}

