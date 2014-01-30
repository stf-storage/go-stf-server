// +build redis

package stf

import (
  "fmt"
  "io"
  "os"
  "os/exec"
  "syscall"
  "time"
  "github.com/lestrrat/go-tcptest"
  _ "github.com/vmihailenco/redis/v2"
)

func (self *TestEnv) startQueue() {
  var cmd *exec.Cmd
  tt, err := tcptest.Start(func(port int) {
    path, err := exec.LookPath("redis-server")
    if err != nil {
      self.Test.Errorf("Failed to find redis-server: %s", err)
      self.Test.FailNow()
    }

    out, err := os.OpenFile("redis.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
    cmd = exec.Command(path, "--port", fmt.Sprintf("%d", port), "--loglevel", "verbose")
    cmd.SysProcAttr = &syscall.SysProcAttr {
      Setpgid: true,
    }
    stderrpipe, err := cmd.StderrPipe()
    if err != nil {
      self.FailNow("Failed to open pipe to stderr")
    }
    stdoutpipe, err := cmd.StdoutPipe()
    if err != nil {
      self.FailNow("Failed to open pipe to stdout")
    }

    go io.Copy(out, stderrpipe)
    go io.Copy(out, stdoutpipe)
    err = cmd.Run()
    if err != nil {
      self.Test.Logf("Command %v exited: %s", cmd.Args, err)
    }
  }, 5 * time.Second)

  if err != nil {
    self.Test.Fatalf("Failed to start redis-server: %s", err)
  }

  addr := fmt.Sprintf("127.0.0.1:%d", tt.Port())
  self.QueueConfig = &QueueConfig{
    Addr: addr,
  }

  self.AddGuard(func() {
    cmd.Process.Signal(syscall.SIGTERM)
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

