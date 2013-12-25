package worker

import (
  "errors"
  "fmt"
  "github.com/stf-storage/go-stf-server"
  "log"
  "sync"
  "time"
)

var ErrNothingDequeued = errors.New("Could not find any jobs")
type WorkerFetcher struct {
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
  name        string,
  config      *stf.Config,
  jobChan     chan *stf.WorkerArg,
  tablename   string,
  timeout     int,
  waiter      *sync.WaitGroup,
) *WorkerFetcher {
  return &WorkerFetcher {
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

func (self *WorkerFetcher) Start() {
  self.Waiter.Add(1)
  go func(name string, w *sync.WaitGroup, jobChan chan *stf.WorkerArg, controlChan chan bool) {
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
      job, err := self.Dequeue()
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
  }(self.Name, self.Waiter, self.JobChan, self.ControlChan)
}

func (self *WorkerFetcher) Dequeue() (*stf.WorkerArg, error) {
  qdbConfig := self.Config.QueueDBList

  sql := fmt.Sprintf(
    "SELECT args, created_at FROM %s WHERE queue_wait(?, ?)",
    self.QueueTableName,
  )

  max := len(qdbConfig)
  var arg stf.WorkerArg
  for i := 0; i < max; i++ {
    idx := self.CurrentQueueIdx
    self.CurrentQueueIdx++
    if self.CurrentQueueIdx >= max {
      self.CurrentQueueIdx = 0
    }
    config := qdbConfig[idx]
    db, err := stf.ConnectDB(config)

    if err != nil {
      continue
    }

    row := db.QueryRow(sql, self.QueueTableName, self.QueueTimeout)

    err = row.Scan(&arg.Arg, &arg.CreatedAt)
    db.Exec("SELECT queue_end()") // Call this regardless

    if err != nil {
      continue
    }

    return &arg, nil
  }

  return nil, ErrNothingDequeued
}

