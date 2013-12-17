package worker

import (
  "stf"
  "strconv"
  "sync"
)

type RepairObjectWorker GenericWorker

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

func (self *RepairObjectWorker) GetJobChannel() JobChannel {
  return self.JobChan
}

func (self *RepairObjectWorker) GetControlChannel() ControlChannel {
  return self.ControlChan
}

func (self *RepairObjectWorker) Start(w *sync.WaitGroup) {
  w.Add(1)
  go GenericWorkerJobReceiver(self, w)
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