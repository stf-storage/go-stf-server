// +build q4m

package stf

import (
  "errors"
  "fmt"
  "math/rand"
)

type QueueConfig DatabaseConfig
type Q4MApi struct {
  BaseQueueApi
  QueueDBPtrList  []*DB
}

func (self *Q4MApi) NumQueueDB () int {
  return len(self.QueueDBPtrList)
}

func NewQ4MApi(ctx ContextForQueueApi) (*Q4MApi) {
  // Find the number of queues, get a random queueIdx
  cfg := ctx.Config()
  max := len(cfg.QueueDBList)
  qidx := rand.Intn(max)

  return &Q4MApi { BaseQueueApi { qidx, ctx }, nil }
}

//  ctx.NumQueueDBCount = len(cfg.QueueDBList)
//  ctx.QueueDBPtrList = make([]*sql.DB, ctx.NumQueueDBCount)
func NewQueueApi(ctx ContextForQueueApi) (QueueApiInterface) {
  return NewQ4MApi(ctx)
}

func ConnectQueue(config *QueueConfig) (*DB, error) {
  return ConnectDB(&DatabaseConfig {
    config.Dbtype,
    config.Username,
    config.Password,
    config.ConnectString,
    config.Dbname,
  })
}

// Gets the i-th Queue DB
func (self *Q4MApi) QueueDB(i int) (*DB, error) {
  if self.QueueDBPtrList[i] == nil {
    config := self.ctx.Config().QueueDBList[i]
    db, err := ConnectQueue(config)
    if err != nil {
      return nil, err
    }
    self.QueueDBPtrList[i] = db
  }
  return self.QueueDBPtrList[i], nil
}

func (self *Q4MApi) Ctx() ContextForQueueApi {
  return self.ctx
}

func (self *Q4MApi) Enqueue (queueName string, data string) error {
  closer := LogMark("[Q4MApi.Enqueue]")
  defer closer()

  max := self.NumQueueDB()
  done  := false

  sql   := fmt.Sprintf("INSERT INTO %s (args, created_at) VALUES (?, UNIX_TIMESTAMP())", queueName)

  for i := 0; i < max; i++ {
    qidx := self.currentQueue
    self.currentQueue++
    if self.currentQueue >= max {
      self.currentQueue = 0
    }

    db, err := self.QueueDB(qidx)
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
  closer := LogMark("[Q4MApi.Dequeue]")
  defer closer()

  max := self.NumQueueDB()

  sql := fmt.Sprintf("SELECT args, created_at FROM %s WHERE queue_wait('%s', ?)", queueName, queueName)

  dberr := 0
  // try all queues
  for i := 0; i < max; i++ {
    qidx := self.currentQueue
    self.currentQueue++
    if (self.currentQueue >= max) {
      self.currentQueue = 0
    }

    db, err := self.QueueDB(qidx)
    if err != nil {
      Debugf("Failed to retrieve QueueDB (%d): %s", qidx, err)
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
      Debugf("Failed to fetch from queue on QueueDB (%d): %s", qidx, err)
      // Ugn, try next one
      dberr++
      continue
    }

    Debugf("Fetched next job %+v", arg)
    return &arg, nil
  }

  // if we got here, we were not able to dequeue anything
  var err error
  if dberr > 0 {
    err = ErrNothingToDequeueDbErrors
  } else {
    err = ErrNothingToDequeue
  }
  Debugf("%s", err)
  return nil, err
}
