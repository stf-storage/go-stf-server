package api

import (
	"github.com/stf-storage/go-stf-server/config"
)

type WorkerArg struct {
	Arg       string
	CreatedAt int
}

type ContextForQueueApi interface {
	Config() *config.Config
}

type QueueApiInterface interface {
	Enqueue(string, string) error
	Dequeue(string, int) (*WorkerArg, error)
}

type BaseQueueApi struct {
	ctx ContextForQueueApi
}
