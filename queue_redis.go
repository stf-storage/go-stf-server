// +build redis

package stf

import (
  "math/rand"
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

func (self *RedisApi) RedisDB(i int) (*redis.Client, error) {

  client := self.RedisClients[i]
  if client != nil {
    return client, nil
  }

  qc := self.ctx.Config().QueueDBList[i]
  rc := redis.Options(*qc)
  client = redis.NewTCPClient(&rc)
  self.RedisClients[i] = client

  return client, nil
}

func NewQueueApi(ctx ContextForQueueApi) (QueueApiInterface) {
  return NewRedisApi(ctx)
}

func (self *RedisApi) Enqueue(string, string) error {
  // Lpush
  return nil
}

func (self *RedisApi) Dequeue(string, int) (*WorkerArg, error) {
  // Rpop
  return nil, nil
}

