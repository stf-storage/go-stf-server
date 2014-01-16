// +build redis

package stf

type RedisApi BaseQueueApi
func NewRedisApi(ctx ContextForQueueApi) *RedisApi {
  // Find the number of queues, get a random queueIdx
  max := ctx.NumQueueDB()
  qidx := rand.Intn(max)

  return &RedisApi { qidx, ctx }
}

func (self *RedisApi) Enqueue(string, string) error {
  return nil
}

func (self *RedisApi) Dequeue(string, int) (*WorkerArg, error) {
  return nil, nil
}

