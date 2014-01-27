package worker

import (
  "fmt"
  "time"
  "github.com/stf-storage/go-stf-server"
)

type IntervalFetcher struct {
  tickChan  <-chan time.Time
}

func NewIntervalFetcher(interval time.Duration) (*IntervalFetcher) {
  return &IntervalFetcher {
    time.Tick(interval),
  }
}

func (i *IntervalFetcher) Loop(ctx *stf.Context, jobChan chan *stf.WorkerArg) {
  for {
    t := <-i.tickChan

    jobChan <-&stf.WorkerArg {
      Arg: fmt.Sprintf("%d", t.UnixNano()),
    }
  }
}

type QueueFetcher struct {
  queueName     string
  queueTimeout  int
}

func NewQueueFetcher(queueName string, queueTimeout int) (*QueueFetcher) {
  return &QueueFetcher {
    queueName,
    queueTimeout,
  }
}

func (q *QueueFetcher) Loop(ctx *stf.Context, jobChan chan *stf.WorkerArg) {
  for {
    api := ctx.QueueApi()
    arg, err := api.Dequeue(q.queueName, q.queueTimeout)
    if err != nil {
      continue
    }
    jobChan <-arg
  }
}


