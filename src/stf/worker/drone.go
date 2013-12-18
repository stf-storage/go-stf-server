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

const (
  BIT_ELECTION  = 1
  BIT_BALANCE   = 2
  BIT_RELOAD    = 4
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
  IsLeader        bool
  State           uint8
  LastElection    *time.Time
  LastBalance     *time.Time
  LastReload      *time.Time
  NextCheckState  time.Time
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
    true,
    0,
    nil,
    nil,
    nil,
    time.Now(),
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
      self.CheckState()
      self.ElectLeader()

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
  next := time.Now().Add(5 * time.Second)
  cache.Set("stf.drone.reload",   &next, 0)
  cache.Set("stf.drone.election", &next, 0)
  cache.Set("stf.drone.balance",  &next, 0)
}

func (self *WorkerDrone) ShouldCheckState() bool {
  return self.NextCheckState.Before(time.Now())
}

func (self *WorkerDrone) ShouldHoldElection() bool {
  return self.State & BIT_ELECTION > 0
}

func (self *WorkerDrone) CheckState () {
  self.State = 0

  if ! self.ShouldCheckState() {
    return
  }

  self.NextCheckState = time.Now().Add(stf.RandomDuration(30))
  cache := self.Cache

  keys := []string {
    "stf.drone.reload",
    "stf.drone.election",
    "stf.drone.balance",
  }
  cached, err := cache.GetMulti(keys, func() interface {} { return &time.Time{} })
  if err != nil {
    log.Printf(fmt.Sprintf("Failed to fetch from cache: %s", err))
    return
  }

  var st uint8 = 0

  var tmp interface {}
  var ok  bool
  var whenToReload  *time.Time
  var whenToElect   *time.Time
  var whenToBalance *time.Time

  if tmp, ok = cached[keys[0]]; ok {
    whenToReload = tmp.(*time.Time)
  }

  if whenToReload == nil || self.LastReload == nil || self.LastReload.Before(*whenToReload) {
    st |= BIT_RELOAD
  }

  if tmp, ok = cached[keys[1]]; ok {
    whenToElect = tmp.(*time.Time)
  }

  if whenToElect == nil || self.LastElection == nil || self.LastElection.Before(*whenToElect) {
    st |= BIT_ELECTION
  }

  if tmp, ok = cached[keys[2]]; ok {
    whenToBalance = tmp.(*time.Time)
  }

  if whenToBalance == nil || self.LastBalance == nil || self.LastBalance.Before(*whenToBalance) {
    st |= BIT_BALANCE
  }

  self.State = st
  log.Printf("Next check state at %s", self.NextCheckState)
}

func (self *WorkerDrone) ElectLeader() {
  if ! self.ShouldHoldElection() {
    return
  }

  log.Printf("Holding leader election")

  db := self.MainDB

  var leaderId string
  row := db.QueryRow(`SELECT drone_id FROM worker_election ORDER BY id ASC LIMIT 1`)
  err := row.Scan(&leaderId)
  if err != nil {
    // Fuck. I have no clue what's going on
    panic(fmt.Sprintf("Failed to elect leader: %s", err))
  }

  log.Printf("Election: elected %s as leader", leaderId)
  self.IsLeader = leaderId == self.Id
  if self.IsLeader {
    log.Printf("Elected myself as the leader!")
  }

  t := time.Now()
  self.LastElection = &t
log.Printf("Updated lastElection: %v", self.LastElection)
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