package worker

import (
  "database/sql"
  "log"
  "math/rand"
  "stf"
  "sync"
  "time"
)

const (
  WORKER_STOP   = 0
  WORKER_EXITED = 1
)

type WorkerCommand interface {
  GetType() int
}

type CmdGeneric struct { Type int }
type Cmd1Arg    struct {
  *CmdGeneric
  Arg string
}

func (self *CmdGeneric) GetType() int { return self.Type }

func CmdStop() WorkerCommand {
  return &CmdGeneric { WORKER_STOP }
}
func CmdWorkerExited(id string) WorkerCommand {
  return &Cmd1Arg { &CmdGeneric { WORKER_EXITED }, id }
}

type JobChannel     chan *stf.WorkerArg
type GenericWorker struct {
  Id            string
  Ctx           *WorkerContext
  MaxJobs       int
  ControlChan   WorkerCommChannel
  PrivateChan   WorkerCommChannel
  JobChan       JobChannel
}

type WorkerHandler interface {
  Work(arg *stf.WorkerArg)
  GetId() string
  GetMaxJobs()        int
  GetJobChannel()     JobChannel
  GetPrivateChannel() WorkerCommChannel
  GetControlChannel() WorkerCommChannel
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
  killedByControl := false
  privChan        := handler.GetPrivateChannel()
  ctrlChan        := handler.GetControlChannel()
  id              := handler.GetId()

  // If killedbyControl is true, we were exiting because we've been 
  // asked to do so. In that case we don't notify our exit
  defer func() {
    if ! killedByControl {
      // we weren't killed explicitly, send via our control channel
      // that we've exited for whatever reason
      ctrlChan <- CmdWorkerExited(id)
    }
  }()

  defer w.Done()

  jobChan := handler.GetJobChannel()
  jobCount := 0
  maxJobs := handler.GetMaxJobs()
  ticker := time.Tick(1 * time.Second)
  loop := true
  for loop {
    select {
    case <-privChan:
      loop = false
      killedByControl = true
      break
    case job := <-jobChan:
      jobCount++
      handler.Work(job)
      break
    case <-ticker:
      // no op. This effectively does "sleep unless there's nothing
      // better to do"
    }

    if jobCount >= maxJobs {
      loop = false
    }
  }

  log.Printf("Worker %s exiting", id)
}


