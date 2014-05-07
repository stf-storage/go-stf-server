package drone

import (
	"math/rand"
	"time"
)

type PeriodicTask struct {
	cmdCh				chan struct {} // XXX bogus data, just send signal for now
	maxInterval time.Duration
	randomize   bool
	callback    func(*PeriodicTask)
}
type PeriodicTaskList []*PeriodicTask

func NewPeriodicTask(interval time.Duration, randomize bool, callback func(*PeriodicTask)) *PeriodicTask {
	return &PeriodicTask {
			make(chan struct{}, 1),
			interval,
			randomize,
			callback,
	}
}

func (pt *PeriodicTask) Stop() {
	pt.cmdCh <- struct{}{}
}

func (pt *PeriodicTask) Run() {
	for {
		// next fire time might need to be randomized
		interval := pt.maxInterval
		if pt.randomize {
			interval = time.Duration(rand.Int63n(int64(interval)))
		}

		timer := time.After(interval)
		for loop := true; loop; {
			select {
			case <-pt.cmdCh:
				// owner of this task may ask us to stop
				// bail out.
				return
			case <-timer:
				// next tick!
				pt.callback(pt)
				loop = false
			}
		}
	}
}


