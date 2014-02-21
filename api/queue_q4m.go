// +build q4m

package api

import (
  "errors"
  "fmt"
  "math/rand"
  "github.com/stf-storage/go-stf-server"
  "github.com/stf-storage/go-stf-server/config"
)

type Q4M struct {
  BaseApi
  currentQueue int
  QueueDBPtrList  []*stf.DB
}

func (self *Q4M) NumQueueDB () int {
  return len(self.QueueDBPtrList)
}

func NewQ4M(ctx ContextWithApi) (*Q4M) {
  // Find the number of queues, get a random queueIdx
  cfg := ctx.Config()
  max := len(cfg.QueueDBList)
  qidx := rand.Intn(max)

  return &Q4M { BaseApi { ctx }, qidx, nil }
}

//  ctx.NumQueueDBCount = len(cfg.QueueDBList)
//  ctx.QueueDBPtrList = make([]*sql.DB, ctx.NumQueueDBCount)
func NewQueue(ctx ContextWithApi) (QueueApiInterface) {
  return NewQ4M(ctx)
}

func ConnectQueue(cfg *config.QueueConfig) (*stf.DB, error) {
  return stf.ConnectDB(&config.DatabaseConfig {
    cfg.Dbtype,
    cfg.Username,
    cfg.Password,
    cfg.ConnectString,
    cfg.Dbname,
  })
}

// Gets the i-th Queue DB
func (self *Q4M) QueueDB(i int) (*stf.DB, error) {
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

func (self *Q4M) Ctx() ContextWithApi {
  return self.ctx
}

func (self *Q4M) Enqueue (queueName string, data string) error {
  ctx := self.Ctx()
  closer := ctx.LogMark("[Q4M.Enqueue]")
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
func (self *Q4M) Dequeue (queueName string, timeout int) (*WorkerArg, error) {
  ctx := self.Ctx()
  closer := ctx.LogMark("[Q4M.Dequeue]")
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
