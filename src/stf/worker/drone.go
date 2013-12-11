package worker

import (
  "stf"
  "time"
)

var ANNOUNCE_EXPIRES = 5 * time.Minute

type Drone struct {
  Id string
  ctx *WorkerContext
}

func BootstrapContext() (*WorkerContext, error) {
  ctx, err := stf.BootstrapContext()
  if err != nil {
    return nil, err
  }
  return &WorkerContext { *ctx, nil }, nil
}

func BootstrapWorker (ctx *WorkerContext) *Drone {
  return &Drone { "", ctx }
}

func (self *Drone) Ctx() *WorkerContext {
  return self.ctx
}

func (self *Drone) Start () {
  c := make(chan WorkerCommand)
  h := NewRepairObjectWorker(self.Ctx())
  RunWorker(c, h)
/*
  // This is the channel that is used to notify from the
  // Control thread to our main thread (which spawns workers)
  notifyChan := make(chan bool)

  // Launch the controller thread
  go self.Control(notifyChan)

  for _ = range notifyChan {
    // We got notification, so we should check how many of 
    // each worker we're supposed to be running
    self.SpawnWorkers()
  }
*/
}

func (self *Drone) Control (notifyChan chan bool) {
  // Periodic announce tick
  announceTick := time.Tick(ANNOUNCE_EXPIRES / 2)
//   := time.Tick(10 * time.Minute)

  // The control thread goes into an infinite loop
  for self.ShouldLoop() {
    select {
    case <- announceTick:
      self.Announce()
    }
  }
  // Before we go into the Ticker loop, we need to
  // execute the main process once
/*
  self.CheckState (notifyChan)
  for _ = range c {
    self.CheckState(notifyChan)
  }
*/
}

func (self *Drone) Announce() {
  ctx := self.Ctx()
  db, err := ctx.MainDB()
  if err != nil {
    return
  }
  secs := ANNOUNCE_EXPIRES / time.Second
  _, err = db.Exec("INSERT INTO worker_election (drone_id, expires_at) VALUES (?, UNIX_TIMESTAMP() + ?) ON DUPLICATE KEY UPDATE expires_at = expires_at + ?", self.Id, secs, secs)
}

func (self *Drone) ShouldLoop() bool {
  return true
}

func (self *Drone) CheckState (notifyChan chan bool) {
}

func (self *Drone) SpawnWorkers() {
}
