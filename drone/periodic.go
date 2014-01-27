package drone

import (
  "math/rand"
  "time"
)

type PeriodicTask struct {
  maxInterval time.Duration
  randomize   bool
  next        time.Time
  callback    func()
}
type PeriodicTaskList []*PeriodicTask

func (t *PeriodicTask) ShouldFire(now time.Time) bool {
  return now.After(t.next)
}

func (t *PeriodicTask) SetNext() {
  if t.randomize {
    d := time.Duration(rand.Int63n(int64(t.maxInterval)))
    t.next = time.Now().Add(d)
  } else {
    t.next = time.Now().Add(t.maxInterval)
  }
}

func (t *PeriodicTask) Run() {
  defer t.SetNext()
  t.callback()
}

