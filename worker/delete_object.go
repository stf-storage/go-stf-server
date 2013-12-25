package worker

import (
  "github.com/stf-storage/go-stf-server"
  "reflect"
  "strconv"
  "sync"
)

type DeleteObjectWorker GenericWorker

func NewDeleteObjectWorker(args *HandlerArgs) WorkerCommChannel {
  w := NewDynamicWorker(reflect.TypeOf(DeleteObjectWorker {}), args).(*DeleteObjectWorker)
  w.Start(args.Waiter)
  return w.GetPrivateChannel()
}

func (self *DeleteObjectWorker) GetId() string {
  return self.Id
}

func (self *DeleteObjectWorker) GetMaxJobs() int {
  return self.MaxJobs
}

func (self *DeleteObjectWorker) GetJobChannel() JobChannel {
  return self.JobChan
}

func (self *DeleteObjectWorker) GetPrivateChannel() WorkerCommChannel {
  return self.PrivateChan
}

func (self *DeleteObjectWorker) GetControlChannel() WorkerCommChannel {
  return self.ControlChan
}

func (self *DeleteObjectWorker) Start(w *sync.WaitGroup) {
  w.Add(1)
  go GenericWorkerJobReceiver(self, w)
}

func (self *DeleteObjectWorker) Work(arg *stf.WorkerArg) {
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

  entityApi := loopCtx.EntityApi()
  err = entityApi.RemoveForDeletedObjectId(objectId)
  if err != nil {
    ctx.Debugf("Failed to delete entities for object %d: %s", objectId, err)
  } else {
    loopCtx.TxnCommit()
    loopCtx.Debugf("Deleted entities for object %d", objectId)
  }
}

