package worker

import (
  "github.com/stf-storage/go-stf-server"
  "log"
  "math/rand"
  "reflect"
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

type HandlerAttrs interface {
  GetId()             string
  GetMaxJobs()        int
  GetPrivateChannel() WorkerCommChannel
  GetControlChannel() WorkerCommChannel
}

type PeriodicHandler interface {
  HandlerAttrs
  Work()        time.Time
}

type JobHandler interface {
  HandlerAttrs
  Work(arg *stf.WorkerArg)
  GetJobChannel()     JobChannel
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
  if cfg.Global.Debug {
    ctx.DebugLogPtr = stf.NewDebugLog()
    ctx.DebugLogPtr.Prefix = "GLOBAL"
  }
  return ctx
}

func NewDynamicWorker(workerType reflect.Type, args *HandlerArgs) interface {} {
  if workerType.Kind() != reflect.Struct {
    log.Fatalf("Cannot initialize worker Type %v", workerType)
  }

  w := reflect.New(workerType)

  // w is a pointer. get the actual value
  v := w.Elem()

  privateChan := make(WorkerCommChannel, 1)
  v.FieldByName("Id").SetString(args.Id)
  v.FieldByName("Ctx").Set(reflect.ValueOf(args.Ctx))
  v.FieldByName("MaxJobs").SetInt(int64(args.MaxJobs))
  v.FieldByName("ControlChan").Set(reflect.ValueOf(args.ControlChan))
  v.FieldByName("PrivateChan").Set(reflect.ValueOf(privateChan))
  v.FieldByName("JobChan").Set(reflect.ValueOf(args.JobChan))

  return w.Interface()
}

func GenericPeriodicWorker(handler PeriodicHandler, w *sync.WaitGroup) {
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

  jobCount := 0
  maxJobs := handler.GetMaxJobs()
  loop := true
  next := time.Now()
  for loop {
    select {
    case <-privChan:
      loop = false
      killedByControl = true
      break
    default:
      now := time.Now()
      delta := next.Sub(now)
      if seconds := delta.Seconds(); seconds > 0 {
        stf.RandomSleep(1)
      } else {
        jobCount++
        next = handler.Work()
      }
      break
    }

    if jobCount >= maxJobs {
      loop = false
    }
  }

  log.Printf("Worker %s exiting", id)
}

func GenericWorkerJobReceiver(handler JobHandler, w *sync.WaitGroup) {
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


