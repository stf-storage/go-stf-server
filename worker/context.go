package worker

import (
  "database/sql"
  "fmt"
  "github.com/stf-storage/go-stf-server"
)

type WorkerContext struct {
  stf.GlobalContext
  QueueApiPtr *stf.QueueApi
}
type WorkerLoopContext struct {
  stf.LocalContext
//  GlobalContextPtr *WorkerContext
}

func (self *WorkerContext) QueueApi() *stf.QueueApi {
  if self.QueueApiPtr == nil {
    self.QueueApiPtr = stf.NewQueueApi(self)
  }
  return self.QueueApiPtr
}

func (self *WorkerContext) Cache() *stf.MemdClient {
  if self.CachePtr == nil {
    config := self.Config()
    self.CachePtr = stf.NewMemdClient(config.Memcached.Servers...)
  }
  return self.CachePtr
}

func (self *WorkerContext) MainDB() (*sql.DB, error) {
  if self.MainDBPtr == nil {
    db, err := stf.ConnectDB(&self.Config().MainDB)
    if err != nil {
      return nil, err
    }
    self.MainDBPtr = db
  }
  return self.MainDBPtr, nil
}

func (self *WorkerContext) QueueDB(i int) (*sql.DB, error) {
  if self.QueueDBPtrList[i] == nil {
    config := self.Config().QueueDBList[i]
    db, err := stf.ConnectDB(config)
    if err != nil {
      return nil, err
    }
    self.QueueDBPtrList[i] = db
  }
  return self.QueueDBPtrList[i], nil
}

func (self *WorkerContext) NewLoopContext() *WorkerLoopContext {
  rc := &WorkerLoopContext {
    stf.LocalContext{ GlobalContextPtr: self },
  }

  config := self.Config()
  if config.Global.Debug {
    rc.DebugLogPtr = stf.NewDebugLog()
    rc.DebugLogPtr.Prefix = stf.GenerateRandomId(fmt.Sprintf("%p", rc), 8)

  }
  return rc
}

func (self *WorkerLoopContext) GlobalContext() stf.Context {
  return self.GlobalContextPtr
}

func (self *WorkerLoopContext) Config() *stf.Config {
  return self.GlobalContext().Config()
}

func (self *WorkerLoopContext) MainDB() (*sql.DB, error) {
  return self.GlobalContext().MainDB()
}

func (self *WorkerLoopContext) QueueDB(i int) (*sql.DB, error) {
  return self.GlobalContext().QueueDB(i)
}

func (self *WorkerLoopContext) NumQueueDB() int {
  return 0
}

func (self *WorkerLoopContext) Cache() *stf.MemdClient {
  return self.GlobalContext().Cache()
}

func (self *WorkerLoopContext) BucketApi() *stf.BucketApi {
  if self.BucketApiPtr == nil {
    self.BucketApiPtr = stf.NewBucketApi(self)
  }
  return self.BucketApiPtr
}

func (self *WorkerLoopContext) EntityApi() *stf.EntityApi {
  if self.EntityApiPtr == nil {
    self.EntityApiPtr = stf.NewEntityApi(self)
  }
  return self.EntityApiPtr
}

func (self *WorkerLoopContext) DeletedObjectApi() *stf.DeletedObjectApi {
  if self.DeletedObjectApiPtr == nil {
    self.DeletedObjectApiPtr = stf.NewDeletedObjectApi(self)
  }
  return self.DeletedObjectApiPtr
}

func (self *WorkerLoopContext) ObjectApi() *stf.ObjectApi {
  if self.ObjectApiPtr == nil {
    self.ObjectApiPtr = stf.NewObjectApi(self)
  }
  return self.ObjectApiPtr
}

func (self *WorkerLoopContext) QueueApi() *stf.QueueApi {
  if self.QueueApiPtr == nil {
    self.QueueApiPtr = stf.NewQueueApi(self)
  }
  return self.QueueApiPtr
}

func (self *WorkerLoopContext) StorageApi() *stf.StorageApi {
  if self.StorageApiPtr == nil {
    self.StorageApiPtr = stf.NewStorageApi(self)
  }
  return self.StorageApiPtr
}

func (self *WorkerLoopContext) StorageClusterApi() *stf.StorageClusterApi {
  if self.StorageClusterApiPtr == nil {
    self.StorageClusterApiPtr = stf.NewStorageClusterApi(self)
  }
  return self.StorageClusterApiPtr
}

func (self *WorkerLoopContext) Txn() (*sql.Tx, error) {
  if self.TxnPtr == nil {
    return nil, stf.ErrNoTxnInProgress
  }
  return self.TxnPtr, nil
}

func (self *WorkerLoopContext) TxnBegin() (func(), error) {
  return stf.TxnBeginWith(self)
}

func (self *WorkerLoopContext) TxnCommit() error {
  txn := self.TxnPtr
  if txn != nil {
    self.Debugf("Committing transaction")
    err := txn.Commit()
    if err != nil {
      return err
    }

    self.Debugf("Transaction commited")
    self.SetTxnCommited(true)
    self.SetTxn(nil)
  }
  return nil
}

func (self *WorkerLoopContext) TxnRollback() error {
  txn := self.TxnPtr
  if txn == nil {
    return nil
  }

  if self.TxnCommited {
    return nil
  }

  self.Debugf("Rolling back changes")
  err := txn.Rollback()
  if err != nil {
    return err
  }
  self.Debugf("Transaction rolled back")
  return nil
}

func (self *WorkerLoopContext) Destroy() {
  self.TxnRollback()
}
