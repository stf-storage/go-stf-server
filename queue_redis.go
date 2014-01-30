// +build redis

package stf

import (
  "errors"
  "math/rand"
  "time"
  "github.com/vmihailenco/redis/v2"
)

type QueueConfig redis.Options
type RedisApi struct {
  BaseQueueApi
  RedisClients  []*redis.Client
}
func NewRedisApi(ctx ContextForQueueApi) *RedisApi {
  // Find the number of queues, get a random queueIdx
  cfg := ctx.Config()
  max := len(cfg.QueueDBList)
  qidx := rand.Intn(max)

  return &RedisApi { BaseQueueApi { qidx, ctx }, make([]*redis.Client, max, max) }
}

func (self *RedisApi) NumQueueDB () int {
  return len(self.ctx.Config().QueueDBList)
}

func (self *RedisApi) RedisDB(i int) (*redis.Client, error) {

  client := self.RedisClients[i]
  if client != nil {
    return client, nil
  }

  qc := self.ctx.Config().QueueDBList[i]
  rc := redis.Options(*qc)

  Debugf("Connecting to new Redis server %s", rc.Addr)

  client = redis.NewTCPClient(&rc)
  self.RedisClients[i] = client

  return client, nil
}

func NewQueueApi(ctx ContextForQueueApi) (QueueApiInterface) {
  return NewRedisApi(ctx)
}

func (self *RedisApi) Enqueue(qname string, data string) error {
  // Lpush
  max := self.NumQueueDB()
  for i := 0; i < max; i++ {
    qidx := self.currentQueue
    client, err := self.RedisDB(qidx)

    qidx++
    if qidx >= max {
      qidx = 0
    }
    self.currentQueue = qidx

    if err != nil {
      continue
    }

    _, err = client.LPush(qname, data).Result()
    if err != nil {
      continue
    }

    return nil
  }
  return errors.New("Failed to enqueue into any queue")
}

func (self *RedisApi) Dequeue(qname string, timeout int) (*WorkerArg, error) {
  // Rpop
  max := self.NumQueueDB()
  for i := 0; i < max; i++ {
    qidx := self.currentQueue
    client, err := self.RedisDB(qidx)

    qidx++
    if qidx >= max {
      qidx = 0
    }
    self.currentQueue = qidx

    if err != nil {
      continue
    }

    val, err := client.RPop(qname).Result()
    if err != nil {
      if err == redis.Nil {
        // sleep a bit
        time.Sleep(time.Duration(rand.Int63n(int64(5 * time.Second))))
      }
      continue
    }

    Debugf("val -> %s", val)
    return &WorkerArg{ Arg: val }, nil

  }
  return nil, errors.New("Failed to dequeue from any queue")
}

