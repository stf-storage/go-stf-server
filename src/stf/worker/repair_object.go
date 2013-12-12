package worker

import (
  "stf"
  "strconv"
)

type RepairObjectWorker struct {
  GenericWorker
}

func NewRepairObjectWorker(ctx *WorkerContext) *RepairObjectWorker {
  return &RepairObjectWorker {
    GenericWorker {
      5,
      "queue_repair_object",
      ctx,
    },
  }
}

func (self *RepairObjectWorker) NextJob() (*stf.WorkerArg, error) {
  ctx := self.Ctx()
  ctx.Debugf("Attempting to fetch next job from %s", self.QueueName())
  arg, err := ctx.QueueApi().Dequeue(self.QueueName(), self.Interval())
  if err != nil {
    return nil, err
  }
  return arg, nil
}

func (self *RepairObjectWorker) Work(arg *stf.WorkerArg) {
  objectId, err := strconv.ParseUint(arg.Arg, 10, 64)
  if err != nil {
    return
  }

  // Create a per-loop context
  ctx := self.Ctx() // Note, this is "Global" context
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
    ctx.Debugf("Repaired object %d", objectId)
    loopCtx.TxnCommit()
  }
  
}