package stf

import (
  "errors"
  "fmt"
  "math/rand"
)

type Q4MApi BaseQueueApi

func NewQ4MApi(ctx ContextForQueueApi) (*Q4MApi) {
  // Find the number of queues, get a random queueIdx
  max := ctx.NumQueueDB()
  qidx := rand.Intn(max)

  return &Q4MApi { qidx, ctx }
}

func (self *Q4MApi) Ctx() ContextForQueueApi {
  return self.ctx
}

func (self *Q4MApi) Enqueue (queueName string, data string) error {
  // XXX QueueDB does not have an associated transaction??
  // This breaks design simmetry. Should we fix it?
  ctx   := self.Ctx()

  closer := ctx.LogMark("[Q4MApi.Enqueue]")
  defer closer()

  max := ctx.NumQueueDB()
  done  := false

  sql   := fmt.Sprintf("INSERT INTO %s (args, created_at) VALUES (?, UNIX_TIMESTAMP())", queueName)

  for i := 0; i < ctx.NumQueueDB(); i++ {
    qidx := self.currentQueue
    self.currentQueue++
    if self.currentQueue >= max {
      self.currentQueue = 0
    }

    db, err := ctx.QueueDB(qidx)
    if err != nil {
      continue
    }

    _, err = db.Exec(sql, data)
    if err != nil {
      continue
    }

    done = true
    break
  }

  if ! done {
    return errors.New("Failed to insert to any of the queue databases")
  }

  return nil
}

var ErrNothingToDequeue = errors.New("Could not dequeue anything")
var ErrNothingToDequeueDbErrors = errors.New("Could not dequeue anything (DB errors)")
func (self *Q4MApi) Dequeue (queueName string, timeout int) (*WorkerArg, error) {
  ctx := self.Ctx()

  closer := ctx.LogMark("[Q4MApi.Dequeue]")
  defer closer()

  max := ctx.NumQueueDB()

  sql := fmt.Sprintf("SELECT args, created_at FROM %s WHERE queue_wait('%s', ?)", queueName, queueName)

  dberr := 0
  // try all queues
  for i := 0; i < max; i++ {
    qidx := self.currentQueue
    self.currentQueue++
    if (self.currentQueue >= max) {
      self.currentQueue = 0
    }

    db, err := ctx.QueueDB(qidx)
    if err != nil {
      ctx.Debugf("Failed to retrieve QueueDB (%d): %s", qidx, err)
      // Ugh, try next one
      dberr++
      continue
    }

    var arg WorkerArg
    row := db.QueryRow(sql, timeout)
    err = row.Scan(&arg.Arg, &arg.CreatedAt)

    // End it! End it!
    db.Exec("SELECT queue_end()")

    if err != nil {
      ctx.Debugf("Failed to fetch from queue on QueueDB (%d): %s", qidx, err)
      // Ugn, try next one
      dberr++
      continue
    }

    ctx.Debugf("Fetched next job %+v", arg)
    return &arg, nil
  }

  // if we got here, we were not able to dequeue anything
  var err error
  if dberr > 0 {
    err = ErrNothingToDequeueDbErrors
  } else {
    err = ErrNothingToDequeue
  }
  ctx.Debugf("%s", err)
  return nil, err
}
