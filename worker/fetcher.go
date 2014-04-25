package worker

import (
	"fmt"
	"github.com/stf-storage/go-stf-server"
	"github.com/stf-storage/go-stf-server/api"
	"time"
)

type IntervalFetcher struct {
	tickChan <-chan time.Time
	running  bool
}

func NewIntervalFetcher(interval time.Duration) *IntervalFetcher {
	return &IntervalFetcher{
		time.Tick(interval),
		false,
	}
}

func (i *IntervalFetcher) SetRunning(b bool) {
	i.running = false
}

func (i *IntervalFetcher) IsRunning() bool {
	return i.running
}

func (i *IntervalFetcher) Loop(w Worker) {
	defer i.SetRunning(false)
	i.SetRunning(true)

	for w.ActiveSlaves() > 0 {
		t := <-i.tickChan

		w.SendJob(&api.WorkerArg{
			Arg: fmt.Sprintf("%d", t.UnixNano()),
		})
	}
}

type QueueFetcher struct {
	queueName    string
	queueTimeout int
	running      bool
}

func NewQueueFetcher(queueName string, queueTimeout int) *QueueFetcher {
	return &QueueFetcher{
		queueName,
		queueTimeout,
		false,
	}
}

func (q *QueueFetcher) SetRunning(b bool) {
	q.running = false
}

func (q *QueueFetcher) IsRunning() bool {
	return q.running
}

func (q *QueueFetcher) Loop(w Worker) {
	closer := stf.LogMark("[Fetcher:%s]", w.Name())
	defer closer()

	defer q.SetRunning(false)
	q.SetRunning(true)

	ctx := w.Ctx()

	// We should only be dequeuing jobs if have slaves to consume
	// them. Otherwise we'd risk missing them in the air
	for w.ActiveSlaves() > 0 {
		api := ctx.QueueApi()
		arg, err := api.Dequeue(q.queueName, q.queueTimeout)
		if err != nil {
			continue
		}
		w.SendJob(arg)
	}
}
