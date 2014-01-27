package worker

import (
  "sync"
  "github.com/stf-storage/go-stf-server"
)

type Fetcher interface {
  Loop(*stf.Context, chan *stf.WorkerArg)
}

type BaseWorker struct {
  loop          bool
  activeSlaves  int
  maxSlaves     int
  ctx           *stf.Context
  waiter        *sync.WaitGroup
  fetcher       Fetcher
  CmdChan       chan WorkerCmd
  JobChan       chan *stf.WorkerArg
  WorkCb        func(*stf.WorkerArg) error
}

type WorkerCmd int
var (
  CmdWorkerSlaveSpawn   = WorkerCmd(1)
  CmdWorkerSlaveStop    = WorkerCmd(2)
  CmdWorkerSlaveStarted = WorkerCmd(3)
  CmdWorkerSlaveExited  = WorkerCmd(4)
  CmdWorkerReload       = WorkerCmd(5)
  CmdWorkerFetcherSpawn = WorkerCmd(6)
  CmdWorkerFetcherExited = WorkerCmd(7)
)

func NewBaseWorker(f Fetcher) (*BaseWorker) {
  return &BaseWorker {
    true,
    0,
    0,
    nil,
    &sync.WaitGroup {},
    f,
    make(chan WorkerCmd, 16),
    make(chan *stf.WorkerArg, 16),
    nil,
  }
}

func (w *BaseWorker) Run() {
  config, err := stf.BootstrapConfig()
  if err != nil {
    stf.Debugf("Failed to load config: %s", err)
    return
  }

  w.ctx = stf.NewContext(config)

  w.waiter.Add(1)
  go w.MainLoop()

  w.CmdChan <-CmdWorkerSlaveSpawn
  w.CmdChan <-CmdWorkerFetcherSpawn

  w.waiter.Wait()
  stf.Debugf("Worker exiting")
}

func (w *BaseWorker) MainLoop() {
  defer w.waiter.Done()
  for {
    cmd := <-w.CmdChan
    w.HandleCommand(cmd)
  }
}

func (w *BaseWorker) HandleCommand(cmd WorkerCmd) {
  switch cmd {
  case CmdWorkerReload:
    w.ReloadConfig()
  case CmdWorkerFetcherSpawn:
    w.FetcherSpawn()
  case CmdWorkerFetcherExited:
    w.CmdChan <-CmdWorkerFetcherSpawn
  case CmdWorkerSlaveStop:
    w.SlaveStop()
  case CmdWorkerSlaveSpawn:
    w.SlaveSpawn()
  case CmdWorkerSlaveStarted:
    w.activeSlaves++
  case CmdWorkerSlaveExited:
    w.activeSlaves--
    w.CmdChan <-CmdWorkerSlaveSpawn
  }
}

func (w *BaseWorker) ReloadConfig() {
  // Get the number of slaves that we need to spawn.
  // If this number is different than the previous state,
  // send myself CmdWorkerSlaveStop or CmdWorkerStartSlave
  if ! w.loop {
    return
  }

  oldMaxSlaves := w.maxSlaves

  diff := oldMaxSlaves - w.activeSlaves;
  if diff > 0 {
    for i := 0; i < diff; i++ {
      w.CmdChan <-CmdWorkerSlaveStop
    }
  } else if diff < 0 {
    diff = diff * -1
    for i := 0; i < diff; i++ {
      w.CmdChan <-CmdWorkerSlaveSpawn
    }
  }
}

func (w *BaseWorker) FetcherSpawn() {
  if ! w.loop {
    return
  }

  go func() {
    w.waiter.Add(1)
    defer func() { w.CmdChan <-CmdWorkerFetcherExited }()
    w.fetcher.Loop(w.ctx, w.JobChan)
  }()
}

func (w *BaseWorker) SlaveStop() {
  if ! w.loop {
    return
  }

  w.JobChan <-nil
}

func (w *BaseWorker) SlaveSpawn() {
  // Bail out if we're supposed to be exiting
  if ! w.loop {
    return
  }

  go w.SlaveLoop()
}

func (w *BaseWorker) SlaveLoop() {
  w.CmdChan <-CmdWorkerSlaveStarted
  // if we exited, spawn a new one
  defer func() { w.CmdChan <-CmdWorkerSlaveExited }()

  stf.Debugf("New slave starting...")
  for {
    job := <-w.JobChan
    if job == nil {
      break // Bail out of loop
    }
    w.WorkCb(job)
  }
}
