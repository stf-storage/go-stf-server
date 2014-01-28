package drone

import (
  "bufio"
  "os"
  "os/exec"
  "syscall"
  "time"
  "github.com/stf-storage/go-stf-server"
)

type Minion struct {
  killed bool
  drone *Drone
  cmd *exec.Cmd
  makeCmd func() (*exec.Cmd)
}

func (m *Minion) Run() {
  if m.cmd == nil {
    go m.run()
  }
}

type pipeConnector struct {
  out *os.File
  rdr *bufio.Reader
}

func (p *pipeConnector) connect() {
  for {
    str, err := p.rdr.ReadBytes('\n')
    if str != nil {
      p.out.Write(str)
      p.out.Sync()
    }

    if err != nil {
      break
    }
  }
}

func (m *Minion) TailOutput() {
  cmd := m.cmd
  if cmd == nil {
    return
  }
  // We want the output from our child processes, too!
  stderrpipe, err := cmd.StderrPipe()
  if err != nil {
    return
  }
  stdoutpipe, err := cmd.StdoutPipe()
  if err != nil {
    return
  }
  pipes := []*pipeConnector {
    { os.Stdout, bufio.NewReader(stdoutpipe) },
    { os.Stderr, bufio.NewReader(stderrpipe) },
  }

  for _, p := range pipes {
    go p.connect()
  }
}

func (m *Minion) run() {
  defer func() {
    m.cmd = nil
    if ! m.killed {
      time.Sleep(1 * time.Second)
      m.drone.SpawnMinions()
    }
  }()
  m.cmd = m.makeCmd()
  stf.Debugf("Running command: %v", m.cmd.Args)

  m.TailOutput()
  if cmd := m.cmd; cmd != nil {
    err := cmd.Run() // BLOCKS!
    if err != nil {
      stf.Debugf("Command %v exited: %s", cmd.Args, err)
    }
  }
}

func (m *Minion) NotifyReload() {
  if m.cmd != nil && m.cmd.Process != nil {
    stf.Debugf("Notifying %v (%d)", m.cmd.Args, m.cmd.Process.Pid)
    p := m.cmd.Process
    p.Signal(syscall.SIGHUP)
  }
}

func (m *Minion) Kill() {
  m.killed = true
  if m.cmd != nil && m.cmd.Process != nil {
    stf.Debugf("Killing %v (%d)", m.cmd.Args, m.cmd.Process.Pid)
    m.cmd.Process.Kill()
  }
}


