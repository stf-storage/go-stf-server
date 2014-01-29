package worker

import (
  "github.com/stf-storage/go-stf-server"
  "strconv"
)

type RepairObjectWorker struct {
  *BaseWorker
}

func NewRepairObjectWorker() (*RepairObjectWorker) {
  f := NewQueueFetcher("queue_repair_object", 1)
  w := &RepairObjectWorker { NewBaseWorker("RepairObject", f) }
  w.WorkCb = w.Work
  return w
}

func (self *RepairObjectWorker) Work(arg *stf.WorkerArg) (err error) {
  objectId, err := strconv.ParseUint(arg.Arg, 10, 64)
  if err != nil {
    return
  }
  defer func() {
    if  err != nil {
      stf.Debugf("Processed object %d", objectId)
    } else {
      stf.Debugf("Failed to process object %d: %s", objectId, err)
    }
  }()

  ctx := self.ctx
  closer, err := ctx.TxnBegin()
  if err != nil {
    return
  }
  defer closer()

  objectApi := ctx.ObjectApi()
  err = objectApi.Repair(objectId)
  if err != nil {
    stf.Debugf("Failed to repair %d: %s", objectId, err)
  } else {
    ctx.TxnCommit()
    stf.Debugf("Repaired object %d", objectId)
  }
  err = nil
  return
}