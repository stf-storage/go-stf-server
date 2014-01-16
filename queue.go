package stf

import (
  "database/sql"
)

type WorkerArg struct {
  Arg string
  CreatedAt int
}

type ContextForQueueApi interface {
  NumQueueDB() int
  QueueDB(int) (*sql.DB, error)
  Debugf(string, ...interface{})
  LogMark(string, ...interface{}) func()
}

type QueueApiInterface interface {
  Enqueue (string, string) error
  Dequeue(string, int) (*WorkerArg, error)
}

type BaseQueueApi struct {
  currentQueue int
  ctx ContextForQueueApi
}

const QUEUE_Q4M = 0
const QUEUE_REDIS = 1

func NewQueueApi(queueType int, ctx ContextForQueueApi) QueueApiInterface {
  switch queueType {
  case QUEUE_Q4M:
    return NewQ4MApi(ctx)
  case QUEUE_REDIS:
    return NewRedisApi(ctx)
  default:
    panic("No such queue type")
  }
}
