package worker

import (
  "stf"
  "strconv"
  "sync"
)

type RepairObjectWorker GenericWorker

func NewRepairObjectWorker(args *HandlerArgs) WorkerCommChannel {
  // This is the channel used to talk to this specific worker
  privateChan := make(WorkerCommChannel, 1)
  worker := &RepairObjectWorker {
    args.Id,
    NewWorkerContext(),
    args.MaxJobs,
    args.ControlChan,
    privateChan,
    args.JobChan,
  }
  worker.Start(args.Waiter)
  return privateChan
}

func (self *RepairObjectWorker) GetId() string {
  return self.Id
}

func (self *RepairObjectWorker) GetMaxJobs() int {
  return self.MaxJobs
}

func (self *RepairObjectWorker) GetJobChannel() JobChannel {
  return self.JobChan
}

func (self *RepairObjectWorker) GetPrivateChannel() WorkerCommChannel {
  return self.PrivateChan
}

func (self *RepairObjectWorker) GetControlChannel() WorkerCommChannel {
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