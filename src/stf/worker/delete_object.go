package worker

import (
  "stf"
  "strconv"
  "sync"
)

type DeleteObjectWorker GenericWorker

func NewDeleteObjectWorker(
  w *sync.WaitGroup,
  jobChan chan *stf.WorkerArg,
) chan bool {

  ctrlChan := make(chan bool)
  worker := &DeleteObjectWorker {
    // This is a DUMMY! DUMMY! I tell you it's a DUMMY!
    NewWorkerContext(),
    ctrlChan,
    jobChan,
  }
  worker.Start(w)
  return ctrlChan
}

func (self *DeleteObjectWorker) GetJobChannel() JobChannel {
  return self.JobChan
}

func (self *DeleteObjectWorker) GetControlChannel() ControlChannel {
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

