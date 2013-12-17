package worker

import (
  "bufio"
  "errors"
  "flag"
  "fmt"
  "log"
  "os"
  "os/exec"
  "os/signal"
  "stf"
  "syscall"
  "time"
)

type WorkerUnitDef struct {
  Name            string
  Command         *exec.Cmd
}

type WorkerDrone struct {
  Config          *stf.Config
  WorkerExitChan  chan *WorkerUnitDef
  WorkerUnitDefs  []*WorkerUnitDef
}

func NewDroneFromArgv () (*WorkerDrone) {
  var configname string
  flag.StringVar(&configname, "config", "etc/config.gcfg", "config file path")
  flag.Parse()

  home := stf.GetHome()
  cfg, err := stf.LoadConfig(home)
  if err != nil {
    log.Fatalf("Failed to config: %s", err)
  }

  return NewDrone(cfg)
}

func NewDrone(cfg *stf.Config) (*WorkerDrone) {
  return &WorkerDrone {
    cfg,
    make(chan *WorkerUnitDef),
    []*WorkerUnitDef{
      &WorkerUnitDef {
        "worker_repair_object",
        nil,
      },
      &WorkerUnitDef {
        "worker_delete_object",
        nil,
      },
    },
  }
}

func (self *WorkerDrone) Start() {
  defer self.KillWorkerUnits()

  for _, t := range self.WorkerUnitDefs {
    cmd, err := self.SpawnWorkerUnit(t)
    if err != nil {
      // If we failed to start out the first command
      // then we should halt
      log.Fatalf("Failed to start WorkerUnit for %s: %s", t.Name, err)
    }

    t.Command = cmd
  }

  loop := true

  // XXX Signal names are not portable... what to do?
  sigChan := make(chan os.Signal, 1)
  signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
  defer func() {signal.Stop(sigChan) }()

  // Channel where we receive process exits
  exitChan := self.WorkerExitChan

  ticker := time.Tick(1 * time.Second)

  for loop {
    select {
    case sig := <-sigChan:
      log.Printf("Received signal %s", sig)
      loop = false
      break // terminate early
    case t := <-exitChan:
      self.SpawnWorkerUnit(t)
      break // 
    default:
      // When we fall here we know that we neither got a signal
      // nor an exit notice. wait for the next ticker, which effectively
      // lets us "sleep"
      <-ticker
    }
  }
}

func (self *WorkerDrone) SpawnWorkerUnit (t *WorkerUnitDef) (*exec.Cmd, error) {
  fullpath, err := exec.LookPath(t.Name)
  if err != nil {
    return nil, errors.New(
      fmt.Sprintf(
        "Failed to find absolute path for '%s': %s",
        t.Name,
        err,
      ),
    )
  }

  cmd := exec.Command(
    fullpath,
    "--config",
    self.Config.FileName,
  )

  // We want the output from our child processes, too!
  stderrpipe, err := cmd.StderrPipe()
  if err != nil {
    return nil, err
  }
  stdoutpipe, err := cmd.StdoutPipe()
  if err != nil {
    return nil, err
  }
  pipes := []struct {
    Out *os.File
    Rdr *bufio.Reader
  } {
    { os.Stdout, bufio.NewReader(stdoutpipe) },
    { os.Stderr, bufio.NewReader(stderrpipe) },
  }

  for _, p := range pipes {
    go func(out *os.File, in *bufio.Reader) {
      for {
        str, err := in.ReadBytes('\n')
        out.Write(str)
        if err != nil {
          return
        }
      }
    }(p.Out, p.Rdr)
  }

  // We need to be able to kill this process at any given
  // moment in time. Therefore, we need to pass back the
  // reference to this command (at least, the process)
  // to the caller
  log.Printf("Starting command %v", cmd.Args)
  err = cmd.Start()
  if err != nil {
    return nil, err
  }

  exitChan := self.WorkerExitChan
  go func () {
    cmd.Wait()
    log.Printf("Exit: %v", cmd.Args)
    exitChan <- t
  }()

  return cmd, nil
}

func (self *WorkerDrone) KillWorkerUnits () {
  for _, t := range self.WorkerUnitDefs {
    if cmd := t.Command; cmd != nil {
      cmd.Process.Kill()
    }
  }
}