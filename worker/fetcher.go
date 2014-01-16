package worker

import (
  "errors"
  "github.com/stf-storage/go-stf-server"
  "log"
  "sync"
  "time"
)

var ErrNothingDequeued = errors.New("Could not find any jobs")
type WorkerFetcher struct {
  Ctx             *WorkerContext
  Name            string
  Config          *stf.Config
  CurrentQueueIdx int
  ControlChan     chan bool
  JobChan         chan *stf.WorkerArg
  QueueTableName  string
  QueueTimeout    int
  Waiter          *sync.WaitGroup
}

func NewWorkerFetcher(
  ctx         *WorkerContext,
  name        string,
  config      *stf.Config,
  jobChan     chan *stf.WorkerArg,
  tablename   string,
  timeout     int,
  waiter      *sync.WaitGroup,
) *WorkerFetcher {
  return &WorkerFetcher {
    ctx,
    name,
    config,
    0,
    make(chan bool),
    jobChan,
    tablename,
    timeout,
    waiter,
  }
}

func (self *WorkerFetcher) Stop() {
  log.Printf("Stop: Sending notice to fetcher")
  self.ControlChan <- true
}

func FetcherControlThread(
  name string,
  w *sync.WaitGroup,
  jobChan chan *stf.WorkerArg,
  controlChan chan bool,
  dequeueCb func() (*stf.WorkerArg, error),
) {
  defer w.Done()

  var skipDequeue <-chan time.Time
  loop := true
  for loop {
    select {
    case <-controlChan:
      log.Printf("Received fetcher termination request. Exiting")
      loop = false
      break
    case <-skipDequeue:
      stf.RandomSleep(1)
    default:
      stf.RandomSleep(1)
    }

    if ! loop {
      break
    }

    // Go and dequeue
    job, err := dequeueCb()
    switch err {
    case nil:
      jobChan <- job

    default:
      // We encountered an error. It's very likely that we are not going
      // to succeed getting the next one. In that case, go listen to the
      // controlChan, but don't fall into the dequeue clause until the
      // next "tick" arrives
      skipDequeue = time.After(500 * time.Millisecond)
    }
  }
  log.Printf("Fetcher for %s exiting", name)
}

func (self *WorkerFetcher) Start() {
  self.Waiter.Add(1)
  go FetcherControlThread(
    self.Name,
    self.Waiter,
    self.JobChan,
    self.ControlChan,
    self.Dequeue,
  )
}

func (self *WorkerFetcher) Dequeue() (*stf.WorkerArg, error) {
  return self.Ctx.QueueApi().Dequeue(self.QueueTableName, self.QueueTimeout)
}

