package worker

import (
  "database/sql"
  "bufio"
  "errors"
  "flag"
  "fmt"
  "github.com/stf-storage/go-stf-server"
  "log"
  "os"
  "os/exec"
  "os/signal"
  "strings"
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
  CommandName     string
  Command         *exec.Cmd
  TotalInstances  int // total instances in the entire system
}

type WorkerDrone struct {
  Id              string
  Config          *stf.Config
  Cache           *stf.MemdClient
  MainDB          *sql.DB
  Loop            bool
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
    true,
    make(chan *WorkerUnitDef),
    []*WorkerUnitDef{
      &WorkerUnitDef {
        "RepairObject",
        "worker_repair_object",
        nil,
        0,
      },
      &WorkerUnitDef {
        "DeleteObject",
        "worker_delete_object",
        nil,
        0,
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

func (self *WorkerDrone) Unregister () {
  log.Printf("Unregister %s", self.Id)
  db := self.MainDB
  db.Exec(`DELETE FROM worker_election WHERE drone_id = ?`, self.Id)
  db.Exec(`DELETE FROM worker_instances WHERE drone_id = ?`, self.Id)
}

func (self *WorkerDrone) Start() {
  defer self.Cleanup()

  log.SetPrefix(fmt.Sprintf("[Drone %d] ", os.Getpid()))

  for _, t := range self.WorkerUnitDefs {
    cmd, err := self.SpawnWorkerUnit(t)
    if err != nil {
      // If we failed to start out the first command
      // then we should halt
      log.Fatalf("Failed to start WorkerUnit for %s: %s", t.Name, err)
    }

    t.Command = cmd
  }

  self.Loop = true

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

  for self.Loop {
    select {
    case sig := <-sigChan:
      log.Printf("Received signal %s", sig)
      self.Loop = false
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
      self.Reload()
      if self.ShouldRebalance() {
        if self.IsLeader {
          self.Rebalance()
        }
        self.NotifyWorkers()
      }

      // When we fall here we know that we neither got a signal
      // nor an exit notice. sleep and wait
      stf.RandomSleep(1)
    }
  }
  self.Loop = false // sanity
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

func (self *WorkerDrone) DeleteExpired() {
  log.Printf("Deleting expired drones")
  db := self.MainDB

  db.Exec(
    `DELETE FROM worker_election WHERE expires_at < UNIX_TIMESTAMP() AND drone_id != ?`,
    self.Id,
  )
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

func (self *WorkerDrone) ShouldRebalance() bool {
  return self.State & BIT_BALANCE > 0
}

func (self *WorkerDrone) ShouldReload() bool {
  return self.State & BIT_RELOAD > 0
}

func (self *WorkerDrone) CheckState () {
  self.State = 0
  if ! self.ShouldCheckState() {
    return
  }

  self.DeleteExpired()

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
  self.IsLeader = (leaderId == self.Id)
  if self.IsLeader {
    log.Printf("Elected myself as the leader!")
  }

  t := time.Now()
  self.LastElection = &t
}

func (self *WorkerDrone) LoadDroneIds() []string {
  db := self.MainDB
  rows, err := db.Query(`SELECT drone_id FROM worker_election ORDER BY id DESC`)
  if err != nil {
    return nil
  }

  drones := []string {}
  for rows.Next() {
    var droneId string
    err = rows.Scan(&droneId)
    if err != nil {
      return nil
    }
    drones = append(drones, droneId)
  }

  return drones
}

func (self *WorkerDrone) Rebalance () {
  if ! self.ShouldRebalance() {
    return
  }

  log.Printf("Rebalacing workers")

  db := self.MainDB

  drones := self.LoadDroneIds()
  if drones == nil {
    log.Printf("No drones?!")
    return
  }

  droneCount := len(drones)

  /* Unlike the original Perl version, this version knows which
   * type of workers it can handle, so we just use this compiled
   * list of worker types
   */
  for _, wu := range self.WorkerUnitDefs {
    /* This is the number of instances that we want in the
     * entire system, which is fetched from the database
     * when Reload() is called
     */
    var instancesPerDrone int
    switch {
    case wu.TotalInstances <= 0 : // safety net
      instancesPerDrone = 0
    case wu.TotalInstances == 1 : // no need to calculate
      instancesPerDrone = 1
    default:
      instancesPerDrone = wu.TotalInstances / droneCount
      if instancesPerDrone < 1 {
        instancesPerDrone = 1
      }
    }

    log.Printf(
      "Total instances for worker %s is %d (%d per drone)",
      wu.Name,
      wu.TotalInstances,
      instancesPerDrone,
    )

    // Keep distributing 
    remaining := wu.TotalInstances
    for i, droneId := range drones {
      var actual int
      switch {
      case remaining < instancesPerDrone:
        actual = remaining
      case i == droneCount - 1: // last one
        actual = remaining
      default:
        actual = instancesPerDrone
      }

      log.Printf(
        "Balance: drone = %s, worker = %s, instances = %d",
        droneId,
        wu.Name,
        actual,
      )

      db.Exec(
        `INSERT INTO worker_instances (drone_id, worker_type, instances)
          VALUES (?, ?, ?)
          ON DUPLICATE KEY UPDATE instances = VALUES(instances)`,
        droneId,
        wu.Name,
        actual,
      )

      remaining -= actual
    }
  }

  self.State |= BIT_RELOAD
  self.Reload()
  self.BroadcastReload()

  t := time.Now()
  self.LastBalance = &t
}

func (self *WorkerDrone) NotifyWorkers() {
  for _, wu := range self.WorkerUnitDefs {
    if cmd := wu.Command; cmd != nil {
      p := cmd.Process
      log.Printf("Sending HUP to process %d to reflect new changes", p.Pid)
      p.Signal(syscall.SIGHUP)
    }
  }
}

func (self *WorkerDrone) Reload() {
  if ! self.ShouldReload() {
    return
  }

  log.Printf("Reloading settings from DB")

  db := self.MainDB
  rows, err := db.Query(`SELECT varname, varvalue FROM config WHERE varname LIKE 'stf.drone.%.instances'`)
  if err != nil {
    log.Printf("Error reloading: %s", err)
    return
  }

  instanceConfig := map[string]string {}
  for rows.Next() {
    var name string
    var value string
    err = rows.Scan(&name, &value)
    if err != nil {
      log.Printf("Error reloading: %s", err)
      return
    }

    // Strip prefix/postfix
    name = strings.TrimPrefix(name, "stf.drone.")
    name = strings.TrimSuffix(name, ".instances")

    instanceConfig[name] = value
  }

  for _, wu := range self.WorkerUnitDefs {
    value, ok := instanceConfig[wu.Name]
    if ok {
      tmp, err := strconv.ParseInt(value, 10, 64)
      if err != nil {
        log.Printf("Error parsing config: %s", err)
      } else {
        wu.TotalInstances = int(tmp)
      }
    }
  }

  t := time.Now()
  self.LastReload = &t
}

func (self *WorkerDrone) SpawnWorkerUnit (t *WorkerUnitDef) (*exec.Cmd, error) {
  fullpath, err := exec.LookPath(t.CommandName)
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
    "--drone",
    self.Id,
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
        if str != nil {
          out.Write(str)
          out.Sync()
        }

        if err != nil {
          break
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
    t.Command = nil
    log.Printf("Exit: %v", cmd.Args)
    if self.Loop {
      log.Printf("We're still in active loop, sending notification to respawn")
      exitChan <- t
    }
  }()

  return cmd, nil
}

func (self *WorkerDrone) KillWorkerUnits () {
  for _, t := range self.WorkerUnitDefs {
    log.Printf("Killing process for %s", t.Name)
    if cmd := t.Command; cmd != nil {
      cmd.Process.Kill()
    }
  }
}

func (self *WorkerDrone) Cleanup() {
  self.BroadcastReload()
  self.KillWorkerUnits()
  self.Unregister()
}
