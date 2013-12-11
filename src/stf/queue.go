package stf

import (
  "database/sql"
  "errors"
  "fmt"
  "math/rand"
)

type WorkerArg struct {
  Arg string
  CreatedAt int
}

type ContextForQueueApi interface {
  NumQueueDB() int
  QueueDB(int) (*sql.DB, error)
  Debugf(string, ...interface{})
}

type QueueApi struct {
  currentQueue int
  ctx ContextForQueueApi
}

func NewQueueApi(ctx ContextForQueueApi) (*QueueApi) {
  // Find the number of queues, get a random queueIdx
  max := ctx.NumQueueDB()
  qidx := rand.Intn(max)

  return &QueueApi { qidx, ctx }
}

func (self *QueueApi) Ctx() ContextForQueueApi {
  return self.ctx
}

func (self *QueueApi) Enqueue (queueName string, data string) error {
  // XXX QueueDB does not have an associated transaction??
  // This breaks design simmetry. Should we fix it?
  ctx   := self.Ctx()
  max := ctx.NumQueueDB()
  done  := false

  sql   := fmt.Sprintf("INSERT INTO %s (arg, created_at) VALUES (?, UNIX_TIMESTAMP())", queueName)

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

func (self *QueueApi) Dequeue (queueName string, timeout int) (*WorkerArg, error) {
  ctx := self.Ctx()
  max := ctx.NumQueueDB()

  sql := fmt.Sprintf("SELECT arg, created_at FROM %s WHERE queue_wait('%s', ?)", queueName)

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
      continue
    }

    var arg WorkerArg
    row := db.QueryRow(sql, timeout)
    err = row.Scan(&arg.Arg, &arg.CreatedAt)
    if err != nil {
      ctx.Debugf("Failed to fetch from queue on QueueDB (%d): %s", qidx, err)
      // Ugn, try next one
      continue
    }

    return &arg, nil
  }

  // if we got here, we were not able to dequeue anything
  return nil, errors.New("Could not dequeue anything")
}
