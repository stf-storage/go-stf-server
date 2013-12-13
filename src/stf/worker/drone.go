package worker

import (
  "os"
  "os/signal"
  "stf"
  "sync"
  "syscall"
  "time"
)

var ANNOUNCE_EXPIRES = 5 * time.Minute

type QueueWatcher func(* WorkerContext, chan bool, func())

type WorkerSpec struct {
  Name string                       // Used just for ID
  QueueTable string
  QueueTimeout int
  Watcher QueueWatcher              // Struct that watches the queue
  JobChan chan *stf.WorkerArg           // Channel to communicate from QueueWatcher (Q4M) -> Workers
  CommandChan map[string]chan *WorkerCommand    // Channel to communicate from Controller -> Workers
}

type Drone struct {
  Id          string
  ctx         *WorkerContext
  Workers     map[string]*WorkerSpec
  Fetchers    map[string]chan bool
  WorkerGroup *sync.WaitGroup
}

func BootstrapContext() (*WorkerContext, error) {
  ctx, err := stf.BootstrapContext()
  if err != nil {
    return nil, err
  }
  return &WorkerContext { *ctx, nil, }, nil
}

func BootstrapWorker (ctx *WorkerContext) *Drone {
  wc := map[string]chan *WorkerCommand {}
  workers := map[string]*WorkerSpec {
    "RepairObject": &WorkerSpec {
      "RepairObject",
      "queue_repair_object",
      60,
      nil,
      make(chan *stf.WorkerArg),
      wc,
    },
  }

  fs := map[string]chan bool {}
  return &Drone {
    "",
    ctx,
    workers,
    fs,
    &sync.WaitGroup{},
  }
}

func (self *Drone) Ctx() *WorkerContext {
  return self.ctx
}

func (self *Drone) Start () {
  // This channel receives signals from the outside world
  // It's also the condvar (sort of) that blocks the main
  // thread that allows other goroutines to go
  sigChan := make(chan os.Signal, 1)
  signal.Notify(
    sigChan,
    syscall.SIGTERM,
    syscall.SIGINT,
    syscall.SIGHUP,
  )

  for i := 0; i < 10; i++ {
    self.SpawnWorker("RepairObject")
  }

  // When we receive a SIGHUP, we kill just reload our
  // settings, and restart, so we do this in a loop
  ctx := self.Ctx()
  loop := true
  for loop {
    ctx.Debugf("Wait for signal")
    sig := <-sigChan
    switch sig {
    case syscall.SIGHUP:
      panic("Unimplemented")
    case syscall.SIGTERM, syscall.SIGINT:
      // Tell the workers to stop, and then bail
      // XXX This needs to be a broadcast operation
      ctx.Debugf("Received TERM/INT")
      for _, spec := range self.Workers {
        for _, c := range spec.CommandChan {
          c <- CmdStop
        }
      }

      for _, c := range self.Fetchers {
        c <- true
      }
      loop = false
      break
    default:
      ctx.Debugf("Received unknown signal")
    }
  }
  // Just to be nice
  signal.Stop(sigChan)

  // Wait here for all the workers to terminate
  self.WorkerGroup.Wait()
}

func (self *Drone) SpawnWorker (name string) {
  ctx := self.Ctx()

  closer := ctx.LogMark("[Drone.SpawnWorker]")
  defer closer()

  spec := self.Workers[name]

  // Are we already watching this queue?
  if spec.Watcher == nil {
    ctx.Debugf("Starting queue watcher for %s", name)
    self.SpawnWatcher(name)
  }

  workerId := stf.GenerateRandomId("worker", 40)

  cmdChan := make(chan *WorkerCommand)

  // Add to spec
  spec.CommandChan[workerId] = cmdChan

  wg := self.WorkerGroup
  w  := &Worker {
    workerId,
    func () {
      wg.Done()
      delete(spec.CommandChan, workerId)
    },
    cmdChan,
    spec.JobChan,
    NewRepairObjectWorker(self.Ctx()),
  }

  wg.Add(1)
  go w.Run()
}

func (self *Drone) SpawnWatcher (name string) {
  ctx := self.Ctx()

  closer := ctx.LogMark("[Drone.SpawnWatcher]")
  defer closer()

  spec := self.Workers[name]

  table   := spec.QueueTable
  timeout := spec.QueueTimeout
  if timeout < 1 {
    timeout = 60
  }

  fetcherId := stf.GenerateRandomId("fetcher", 40)

  haltChan := make(chan bool)
  self.Fetchers[fetcherId] = haltChan
  fetcher := func (ctx *WorkerContext, haltChan chan bool, finalizer func()) {
    ctx.Debugf("Starting dequeue loop")
    defer finalizer()

    loop := true
    for loop {
      select {
      case _ = <-haltChan:
        loop = false
        break
      default:
        arg, err := ctx.QueueApi().Dequeue(table, timeout)
        switch err {
        case nil:
          spec.JobChan <- arg
        case stf.ErrNothingToDequeueDbErrors:
          time.Sleep(10 * time.Second)
        }
      }
    }
    ctx.Debugf("Fetcher exiting")
  }

  finalizer := func() {
    delete(self.Fetchers, fetcherId)
  }

  spec.Watcher = fetcher

  ctx.Debugf("Starting goroutine for fetcher")
  go fetcher(ctx, haltChan, finalizer)
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
