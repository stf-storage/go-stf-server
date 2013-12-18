package worker

import (
  "database/sql"
  "bufio"
  "errors"
  "flag"
  "fmt"
  "log"
  "os"
  "os/exec"
  "os/signal"
  "strings"
  "stf"
  "strconv"
  "syscall"
  "time"
)

type WorkerUnitDef struct {
  Name            string
  Command         *exec.Cmd
}

type WorkerDrone struct {
  Id              string
  Config          *stf.Config
  Cache           *stf.MemdClient
  MainDB          *sql.DB
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
  hostname, err := os.Hostname()
  if err != nil {
    panic("Could not get hostname")
  }

  id := strings.Join(
    []string {
      hostname,
      strconv.FormatInt(int64(os.Getpid()), 10),
      stf.GenerateRandomId("drone", 8),
    },
    ".",
  )

  cache := stf.NewMemdClient(cfg.Memcached.Servers...)
  db, err := stf.ConnectDB(&cfg.MainDB)
  if err != nil {
    panic(fmt.Sprintf("Could not connect to main database: %s", err))
  }

  return &WorkerDrone {
    id,
    cfg,
    cache,
    db,
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

  // Ticker to announce our presence
  announceTick := time.Tick(1 * time.Minute)
  self.Announce()

  // Initial announce, so we should tell the leader to reload
  self.BroadcastReload()

  for loop {
    select {
    case sig := <-sigChan:
      log.Printf("Received signal %s", sig)
      loop = false
      break // terminate early
    case <-announceTick:
      // Announce our presence every so often
      self.Announce()
    case t := <-exitChan:
      self.SpawnWorkerUnit(t)
      break //
    default:
      // When we fall here we know that we neither got a signal
      // nor an exit notice. sleep and wait
      stf.RandomSleep()
    }
  }
}

func (self *WorkerDrone) Announce() {
  log.Printf("Announce %s", self.Id)

  db  := self.MainDB
  _, err := db.Exec(
    `INSERT INTO worker_election (drone_id, expires_at)
      VALUES (?, UNIX_TIMESTAMP() + 300) 
      ON DUPLICATE KEY UPDATE expires_at = UNIX_TIMESTAMP() + 300`,
    self.Id,
  )

  if err != nil {
    panic(fmt.Sprintf("Failed to announce: %s", err))
  }
}

func (self *WorkerDrone) BroadcastReload() {
  log.Printf("Broadcast reload")

  cache := self.Cache
  next := time.Unix(0, 0)
  cache.Set("stf.drone.reload",   next, 0)
  cache.Set("stf.drone.election", next, 0)
  cache.Set("stf.drone.balance",  next, 0)
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