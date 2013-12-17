package worker

import (
  "database/sql"
  "math/rand"
  "stf"
  "sync"
  "time"
)

const (
  WORKER_STOP    = 0
)

type WorkerCommand struct {
  Type int
}

var CmdStop = &WorkerCommand { WORKER_STOP }

type JobChannel     chan *stf.WorkerArg
type ControlChannel chan bool
type GenericWorker struct {
  Ctx           *WorkerContext
  ControlChan   ControlChannel
  JobChan       JobChannel
}

type WorkerHandler interface {
  Work(arg *stf.WorkerArg)
  GetJobChannel()     JobChannel
  GetControlChannel() ControlChannel
}

func NewWorkerContext () *WorkerContext {
  rand.Seed(time.Now().UTC().UnixNano())
  home := stf.GetHome()
  ctx := &WorkerContext{
    stf.GlobalContext {
      HomeStr: home,
    },
    nil,
  }

  cfg, err  := ctx.LoadConfig()
  if err != nil {
    return nil
  }

  ctx.ConfigPtr = cfg
  ctx.NumQueueDBCount = len(cfg.QueueDBList)
  ctx.QueueDBPtrList = make([]*sql.DB, ctx.NumQueueDBCount)

  if cfg.Global.Debug {
    ctx.DebugLogPtr = stf.NewDebugLog()
    ctx.DebugLogPtr.Prefix = "GLOBAL"
  }
  return ctx
}

func GenericWorkerJobReceiver(handler WorkerHandler, w *sync.WaitGroup) {
  defer w.Done()

  jobChan := handler.GetJobChannel()
  ctrlChan := handler.GetControlChannel()
  ticker := time.Tick(1 * time.Second)
  loop := true
  for loop {
    select {
    case job := <-jobChan:
      handler.Work(job)
    case <-ctrlChan:
      loop = false
      break
    case <-ticker:
      // no op. This effectively does "sleep unless there's nothing
      // better to do"
    }
  }
}


