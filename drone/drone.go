package drone

import (
  "fmt"
  "log"
  "math/rand"
  "os"
  "os/exec"
  "os/signal"
  "sync"
  "syscall"
  "time"
  "github.com/stf-storage/go-stf-server"
)

type Drone struct {
  id      string
  ctx     *stf.Context
  loop    bool
  leader  bool
  tasks   []*PeriodicTask
  minions []*Minion
  waiter  *sync.WaitGroup
  CmdChan chan DroneCmd
}

type DroneCmd int
var (
  CmdStopDrone    = DroneCmd(-1)
  CmdAnnounce     = DroneCmd(0)
  CmdSpawnMinion  = DroneCmd(1)
  CmdReloadMinion = DroneCmd(2)
  CmdCheckState   = DroneCmd(3)
  CmdElection     = DroneCmd(4)
  CmdRebalance    = DroneCmd(5)
  CmdExpireDrone  = DroneCmd(6)
)

func (c DroneCmd) ToString() string {
  switch c {
  case CmdStopDrone:
    return "CmdStopDrone"
  case CmdAnnounce:
    return "CmdAnnounce"
  case CmdSpawnMinion:
    return "CmdSpawnMinion"
  case CmdReloadMinion:
    return "CmdReloadMinion"
  case CmdCheckState:
    return "CmdCheckState"
  case CmdElection:
    return "CmdElection"
  case CmdRebalance:
    return "CmdRebalance"
  case CmdExpireDrone:
    return "CmdExpireDrone"
  default:
    return fmt.Sprintf("UnknownCommand(%d)", c)
  }
}

func NewDrone(config *stf.Config) (*Drone) {
  host, err := os.Hostname()
  if err != nil {
    log.Fatalf("Failed to get hostname: %s", err)
  }
  pid := os.Getpid()
  id := fmt.Sprintf("%s.%d.%d", host, pid, rand.Int31())
  return &Drone {
    id: id,
    ctx: stf.NewContext(config),
    loop: true,
    leader: false,
    tasks: nil,
    minions: nil,
    waiter: &sync.WaitGroup {},
    CmdChan: make(chan DroneCmd, 1),
  }
}

func (d *Drone) Loop() bool {
  return d.loop
}

func (d *Drone) Run() {
  defer func () {
    d.Unregister()
    for _, m := range d.Minions() {
      m.Kill()
    }
  }()

  d.waiter.Add(1) // for MainLoop
  go d.MainLoop()
  go d.TimerLoop()
  go d.WaitSignal()

  // We need to kickstart:
  d.CmdChan <-CmdSpawnMinion

  d.waiter.Wait()

  stf.Debugf("Drone %s exiting...", d.id)
}

func (d *Drone) WaitSignal() {
  sigchan := make(chan os.Signal, 1)
  signal.Notify(sigchan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

  for d.Loop() {
    sig := <-sigchan
    switch sig {
    case syscall.SIGTERM, syscall.SIGINT:
      d.CmdChan <-CmdStopDrone
    case syscall.SIGHUP:
      d.CmdChan <-CmdReloadMinion
    }
  }
}

func (d *Drone) makeAnnounceTask() (*PeriodicTask) {
  return &PeriodicTask {
    30 * time.Second,
    true,
    time.Time{},
    func() { d.CmdChan <-CmdAnnounce },
  }
}

func (d *Drone) makeCheckStateTask() (*PeriodicTask) {
  return &PeriodicTask {
    60 * time.Second,
    true,
    time.Time{},
    func() { d.CmdChan <-CmdCheckState },
  }
}

func (d *Drone) makeElectionTask() (*PeriodicTask) {
  return &PeriodicTask {
    300 * time.Second,
    false,
    time.Time{},
    func() { d.CmdChan <-CmdElection },
  }
}

func (d *Drone) makeExpireTask() (*PeriodicTask) {
  return &PeriodicTask {
    60 * time.Second,
    true,
    time.Time{},
    func() { d.CmdChan <-CmdExpireDrone },
  }
}

func (d *Drone) makeRebalanceTask() (*PeriodicTask) {
  return &PeriodicTask {
    60 * time.Second,
    true,
    time.Time{},
    func() { d.CmdChan <-CmdRebalance },
  }
}

func (d *Drone) PeriodicTasks() ([]*PeriodicTask) {
  if d.tasks != nil {
    return d.tasks
  }

  if d.leader {
    d.tasks = make([]*PeriodicTask, 5)
  } else {
    d.tasks = make([]*PeriodicTask, 4)
  }

  d.tasks[0] = d.makeAnnounceTask()
  d.tasks[1] = d.makeCheckStateTask()
  d.tasks[2] = d.makeElectionTask()
  d.tasks[3] = d.makeExpireTask()
  if d.leader {
    d.tasks[4] = d.makeRebalanceTask()
  }

  return d.tasks
}

func (d *Drone) Minions() []*Minion {
  if d.minions != nil {
    return d.minions
  }

  cmds := []string {
//    "stf-worker-adaptive_throttler",
//    "stf-worker-delete_bucket",
    "stf-worker-delete_object",
    "stf-worker-repair_object",
    "stf-worker-storage_health",
  }

  d.minions = make([]*Minion, len(cmds))
  for i, cmd := range cmds {
    d.minions[i] = &Minion {
      drone: d,
      cmd: nil,
      makeCmd: func() *exec.Cmd {
        return exec.Command(cmd, "--config", d.ctx.Config().FileName)
      },
    }
  }
  return d.minions
}

func (d *Drone) NotifyMinions() {
  for _, m := range d.Minions() {
    m.NotifyReload()
  }
}

func (d *Drone) TimerLoop() {
  // Periodically wake up to check if we have anything to do
  c := time.Tick(5 * time.Second)

  // fire once in the beginning
  for _, task := range d.PeriodicTasks() {
    task.Run()
  }

  for {
    t := <-c
    for _, task := range d.PeriodicTasks() {
      if task.ShouldFire(t) {
        task.Run()
      }
    }
  }
}
func (d *Drone) MainLoop() {
  defer d.waiter.Done()

  for d.Loop() {
    cmd := <-d.CmdChan
    d.HandleCommand(cmd)
  }
}

func (d *Drone) HandleCommand(cmd DroneCmd) {
  switch cmd {
  case CmdStopDrone:
    d.loop = false
  case CmdAnnounce:
    d.Announce()
  case CmdSpawnMinion:
    d.SpawnMinions()
  case CmdReloadMinion:
    d.NotifyMinions()
  case CmdCheckState:
    d.CheckState()
  case CmdExpireDrone:
    d.ExpireDrones()
  }
}

func (d *Drone) Announce() error {
  db, err := d.ctx.MainDB()
  if err != nil {
    return err
  }

  stf.Debugf("Annoucing %s", d.id)
  db.Exec(`
    INSERT INTO worker_election (drone_id, expires_at)
      VALUES (?, UNIX_TIMESTAMP() + 300)
      ON DUPLICATE KEY UPDATE expires_at = UNIX_TIMESTAMP() + 300`,
    d.id,
  )

  return nil
}

func (d *Drone) ExpireDrones() {
  db, err := d.ctx.MainDB()
  if err != nil {
    return
  }

  rows, err := db.Query(`SELECT drone_id FROM worker_election WHERE expires_at <= UNIX_TIMESTAMP()`)
  if err != nil {
    return
  }

  for rows.Next() {
    var id string
    err = rows.Scan(&id)
    if err != nil {
      return
    }
    stf.Debugf("Expiring drone %s", id)
    db.Exec(`DELETE FROM worker_election WHERE drone_id = ?`, id)
    db.Exec(`DELETE FROM worker_instances WHERE drone_id = ?`, id)
  }
}

func (d *Drone) Unregister() error {
  db, err := d.MainDB()
  if err != nil {
    return err
  }

  stf.Debugf("Unregistering drone %s from database", d.id)
  db.Exec(`DELETE FROM worker_election WHERE drone_id = ?`, d.id)
  db.Exec(`DELETE FROM worker_instances WHERE drone_id = ?`, d.id)
  return nil
}

func (d *Drone) CheckState() {
}

func (d *Drone) MainDB() (*stf.DB, error) {
  return d.ctx.MainDB()
}
