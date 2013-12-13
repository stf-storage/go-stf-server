package worker

import (
  "log"
  "math/rand"
  "stf"
  "time"
)

const (
  WORKER_STOP    = 0
)

type WorkerCommand struct {
  Type int
}

var CmdStop = &WorkerCommand { WORKER_STOP }

type Worker struct {
  Id          string
  Finalizer   func()
  CommandChan chan *WorkerCommand
  JobChan     chan *stf.WorkerArg
  Handler     WorkerHandler
}

type WorkerHandler interface {
  Interval()            int
  NextJob ()            (*stf.WorkerArg, error)
  QueueName()           string
  Work (*stf.WorkerArg)
  Debugf(string, ...interface{})
}

type GenericWorker struct {
  IntervalSlot  int
  QueueNameStr string
  ctx *WorkerContext
}

func (self *GenericWorker) Ctx() *WorkerContext {
  return self.ctx
}

func (self *GenericWorker) Interval() int {
  return self.IntervalSlot
}

func (self *GenericWorker) QueueName() string {
  return self.QueueNameStr
}

func (self *GenericWorker) DebugLog() *stf.DebugLog {
  return self.Ctx().DebugLog()
}

func (self *GenericWorker) Debugf (format string, args ...interface {}) {
  if dl := self.DebugLog(); dl != nil {
    dl.Printf(format, args...)
  }
}

func (self *Worker) Run() {
  loop := true

  for loop {
    select {
    case cmd := <-self.CommandChan:
      log.Printf("Received command %+v", cmd)
      switch cmd.Type {
      case WORKER_STOP:
        // Bail out of this loop
        loop = false
        continue
      default:
        log.Printf("Unknown command type = %d. Ignoring", cmd.Type)
      }
    }

    h := self.Handler
    select {
    case job := <-self.JobChan:
      h.Work(job)
    }

    // Wait for it...
    if interval := h.Interval(); interval > 0 {
      // Randomize sleep time
      d := time.Duration(rand.Int63n(int64(interval) * int64(time.Second)))
      time.Sleep(d)
    }
  }

  log.Printf("RunWorker done")
  self.Finalizer()
}

