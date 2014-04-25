package worker

import (
	"github.com/stf-storage/go-stf-server"
	"github.com/stf-storage/go-stf-server/api"
	"strconv"
)

type ReplicateObjectWorker struct {
	*BaseWorker
}

func NewReplicateObjectWorker() *ReplicateObjectWorker {
	f := NewQueueFetcher("queue_replicate", 1)
	w := &ReplicateObjectWorker{NewBaseWorker("ReplicateObject", f)}
	w.WorkCb = w.Work
	return w
}

func (self *ReplicateObjectWorker) Work(arg *api.WorkerArg) (err error) {
	objectId, err := strconv.ParseUint(arg.Arg, 10, 64)
	if err != nil {
		return
	}
	defer func() {
		if err == nil {
			stf.Debugf("Processed object %d", objectId)
		} else {
			stf.Debugf("Failed to process object %d: %s", objectId, err)
		}
	}()

	ctx := self.ctx
	closer, err := ctx.TxnBegin()
	if err != nil {
		return
	}
	defer closer()

	objectApi := ctx.ObjectApi()
	err = objectApi.Repair(objectId)
	if err != nil {
		return
	}

	err = ctx.TxnCommit()
	if err != nil {
		return
	}

	return
}
