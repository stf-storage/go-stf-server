package drone

import (
	"github.com/stf-storage/go-stf-server"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"
)

type Minion struct {
	killed  bool
	drone   *Drone
	cmd     *exec.Cmd
	makeCmd func() *exec.Cmd
}

func (m *Minion) Run() {
	if m.cmd == nil {
		go m.run()
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

	go io.Copy(os.Stdout, stdoutpipe)
	go io.Copy(os.Stderr, stderrpipe)
}

func (m *Minion) run() {
	defer func() {
		m.cmd = nil
		if !m.killed {
			time.Sleep(1 * time.Second)
			m.drone.SendCmd(CmdSpawnMinion)
		}
	}()
	cmd := m.makeCmd()
	m.cmd = cmd

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
	stf.Debugf("Running command: %v", cmd.Args)

	m.TailOutput()
	if cmd != nil {
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
		stf.Debugf("Sending TERM to %v (%d)", m.cmd.Args, m.cmd.Process.Pid)
		m.cmd.Process.Signal(syscall.SIGTERM)
	}
}
