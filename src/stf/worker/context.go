package worker

import (
  "stf"
  "database/sql"
)

type WorkerContext struct {
  stf.GlobalContext
  QueueApiPtr *stf.QueueApi
}
type WorkerLoopContext struct {
  stf.LocalContext
  GlobalContextPtr *WorkerContext
}

func (self *WorkerContext) QueueApi() *stf.QueueApi {
  if self.QueueApiPtr == nil {
    self.QueueApiPtr = stf.NewQueueApi(self)
  }
  return self.QueueApiPtr
}

func (self *WorkerLoopContext) GlobalContext() stf.Context {
  return self.GlobalContextPtr
}

func (self *WorkerLoopContext) Config() *stf.Config {
  return self.GlobalContext().Config()
}

func (self *WorkerLoopContext) MainDB() (*sql.DB, error) {
  return nil, nil
}

func (self *WorkerLoopContext) QueueDB(i int) (*sql.DB, error) {
  return nil, nil
}

func (self *WorkerLoopContext) NumQueueDB() int {
  return 0
}

func (self *WorkerLoopContext) DebugLog() *stf.DebugLog {
  return nil
}

func (self *WorkerLoopContext) Debugf(format string, args ...interface{}) {
}
func (self *WorkerLoopContext) LogMark(format string, args ...interface{}) func(){
  return nil
}

func (self *WorkerLoopContext) BucketApi() *stf.BucketApi {
  return nil
}

func (self *WorkerLoopContext) EntityApi() *stf.EntityApi {
  return nil
}

func (self *WorkerLoopContext) ObjectApi() *stf.ObjectApi {
  return nil
}

func (self *WorkerLoopContext) QueueApi() *stf.QueueApi {
  if self.QueueApiPtr == nil {
    self.QueueApiPtr = stf.NewQueueApi(self)
  }
  return self.QueueApiPtr
}

func (self *WorkerLoopContext) StorageApi() *stf.StorageApi {
  return nil
}

func (self *WorkerLoopContext) StorageClusterApi() *stf.StorageClusterApi {
  return nil
}

func (self *WorkerLoopContext) Cache() *stf.MemdClient {
  return nil
}

func (self *WorkerLoopContext) Txn() (*sql.Tx, error) {
  return nil, nil
}

func (self *WorkerLoopContext) TxnBegin() (func(), error) {
  return nil, nil
}

func (self *WorkerLoopContext) TxnCommit() error {
  return nil
}

func (self *WorkerLoopContext) TxnRollback() error {
  return nil
}

func (self *WorkerLoopContext) Destroy() {
  self.TxnRollback()
}
