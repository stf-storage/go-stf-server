package stf

type WorkerArg struct {
  Arg string
  CreatedAt int
}

type ContextForQueueApi interface {
  Config() *Config
}

type QueueApiInterface interface {
  Enqueue (string, string) error
  Dequeue(string, int) (*WorkerArg, error)
}

type BaseQueueApi struct {
  currentQueue int
  ctx ContextForQueueApi
}

