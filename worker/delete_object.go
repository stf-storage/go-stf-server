package worker

import (
  "github.com/stf-storage/go-stf-server"
  "strconv"
)

type DeleteObjectWorker struct {
  *BaseWorker
}

func NewDeleteObjectWorker() (*DeleteObjectWorker) {
  f := NewQueueFetcher("queue_delete_object", 1)
  w := &DeleteObjectWorker {
    NewBaseWorker(f),
  }
  w.WorkCb = w.Work
  return w
}

func (self *DeleteObjectWorker) Work(arg *stf.WorkerArg) (err error) {
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

  entityApi := ctx.EntityApi()
  err = entityApi.RemoveForDeletedObjectId(objectId)
  if err != nil {
    stf.Debugf("Failed to delete entities for object %d: %s", objectId, err)
  } else {
    ctx.TxnCommit()
    stf.Debugf("Deleted entities for object %d", objectId)
  }

  err = nil
  return
}

