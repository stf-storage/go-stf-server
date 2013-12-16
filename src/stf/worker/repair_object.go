package worker

import (
  "math/rand"
  "stf"
  "strconv"
  "sync"
  "time"
)

type RepairObjectWorker struct {
  Ctx           *WorkerContext
  ControlChan   chan bool
  JobChan       chan *stf.WorkerArg
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
  return ctx
}

func NewRepairObjectWorker(
  w *sync.WaitGroup,
  jobChan chan *stf.WorkerArg,
) chan bool {

  ctrlChan := make(chan bool)
  worker := &RepairObjectWorker {
    // This is a DUMMY! DUMMY! I tell you it's a DUMMY!
    NewWorkerContext(),
    ctrlChan,
    jobChan,
  }
  worker.Start(w)
  return ctrlChan
}

func (self *RepairObjectWorker) Start(w *sync.WaitGroup) {
  w.Add(1)
  go func() {
    defer w.Done()

    ticker := time.Tick(1 * time.Second)
    loop := true
    for loop {
      select {
      case job := <-self.JobChan:
        self.Work(job)
      case <-self.ControlChan:
        loop = false
        break
      case <-ticker:
        // no op
      }
    }
  }()
}

func (self *RepairObjectWorker) Work(arg *stf.WorkerArg) {
  objectId, err := strconv.ParseUint(arg.Arg, 10, 64)
  if err != nil {
    return
  }

  // Create a per-loop context
  ctx := self.Ctx // Note, this is "Global" context
  loopCtx := ctx.NewLoopContext()
  closer, err := loopCtx.TxnBegin()
  if err != nil {
    return
  }
  defer closer()

  objectApi := loopCtx.ObjectApi()
  err = objectApi.Repair(objectId)
  if err != nil {
    ctx.Debugf("Failed to repair %d: %s", objectId, err)
  } else {
    loopCtx.TxnCommit()
    loopCtx.Debugf("Repaired object %d", objectId)
  }
}