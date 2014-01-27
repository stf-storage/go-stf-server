package stf

import (
  "database/sql"
  "errors"
  "io"
  "log"
  "os"
  "strconv"
  "strings"
  _ "github.com/go-sql-driver/mysql"
)

type ApiHolder interface {
  BucketApi()   *BucketApi
  DeletedObjectApi()  *DeletedObjectApi
  EntityApi()   *EntityApi
  ObjectApi()   *ObjectApi
  QueueApi()    QueueApiInterface
  StorageApi()  *StorageApi
  StorageClusterApi() *StorageClusterApi
}


type TxnManager interface {
  TxnBegin() (func(), error)
  TxnCommit()
  TxnRollback()
}

type ContextWithApi interface {
  Cache() *MemdClient
  Txn() (*sql.Tx, error)
  ApiHolder
  TxnManager
}

type Context struct {
  DebugLogPtr     *DebugLog
  HomeStr         string
  bucketapi       *BucketApi
  config          *Config
  deletedobjectapi *DeletedObjectApi
  entityapi       *EntityApi
  objectapi       *ObjectApi
  queueapi        QueueApiInterface
  storageapi      *StorageApi
  storageclusterapi *StorageClusterApi
  CachePtr        *MemdClient
  maindb          *DB
  tx              *sql.Tx
  IdgenPtr        *UUIDGen
}

func (self *Context) Home() string { return self.HomeStr }
func (ctx *Context) LoadConfig() (*Config, error) {
  return LoadConfig(ctx.Home())
}

func NewContext(config *Config) *Context {
  return &Context{
    config: config,
  }
}

func (self *Context) NewScope() (*ScopedContext, error) {
  return NewScopedContext(self.Config())
}

type ScopedContext struct {
  config  *Config
  tx      *sql.Tx
  maindb  *DB
}

func (self *Context) MainDB() (*DB, error) {
  if db := self.maindb; db != nil {
    return db, nil
  }

  db, err := ConnectDB(&self.Config().MainDB)
  if err != nil {
    return nil, err
  }

  self.maindb = db
  return db, nil
}

func (self *Context) Config() *Config {
  return self.config
}

func (self *ScopedContext) EndScope() {}
func NewScopedContext(config *Config) (*ScopedContext, error) {
  return &ScopedContext{
    config,
    nil,
    nil,
  }, nil
}

func (self *Context) TxnBegin() (func(), error) {
  db, err := self.MainDB()
  if err != nil {
    return nil, err
  }

  tx, err := db.Begin()
  if err != nil {
    return nil, err
  }

  self.tx = tx

  return self.TxnRollback, nil
}

func (self *Context) TxnRollback() {
  if tx, _ := self.Txn(); tx != nil {
    tx.Rollback()
  }
}

func (self *Context) TxnCommit() {
  if tx, _ := self.Txn(); tx != nil {
    tx.Commit()
    self.tx = nil
  }
}

var ErrNoTxnInProgress = errors.New("No transaction in progress")
func (self *Context) Txn() (*sql.Tx, error) {
  if self.tx == nil {
    return nil, ErrNoTxnInProgress
  }
  return self.tx, nil
}

func BootstrapContext() (*Context, error) {
  ctx := NewContext(nil)

  cfg, err  := ctx.LoadConfig()
  if err != nil {
    return nil, err
  }

  ctx.config = cfg
  ctx.IdgenPtr = NewIdGenerator(cfg.Dispatcher.ServerId)

  var dbgOutput  io.Writer = os.Stderr
  if dbg := os.Getenv("STF_DEBUG"); dbg != "" {
    // STF_DEBUG='1:path/to/log'
    if strings.Index(dbg, ":") > -1 {
      list := strings.Split(dbg, ":")
      dbg = list[0]
      if len(list) > 1 {
        file, err := os.OpenFile(list[1], os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
        if err != nil {
          log.Fatalf("Failed to open debug log output %s: %s", list[1], err)
        }
        dbgOutput = file
      }
    }
    x, err := strconv.ParseBool(dbg)
    if err == nil {
      cfg.Global.Debug = x
    }
  }

  if cfg.Global.Debug {
    log.SetOutput(dbgOutput)
    ctx.DebugLogPtr = NewDebugLog()
    ctx.DebugLogPtr.Prefix = "GLOBAL"
    ctx.DebugLogPtr.Printf("Starting debug log")
  }

  return ctx, nil
}

func (self *Context) DebugLog() *DebugLog {
  return self.DebugLogPtr
}

func (self *Context) Debugf (format string, args ...interface {}) {
  if dl := self.DebugLog(); dl != nil {
    dl.Printf(format, args...)
  }
}

func (self *Context) IdGenerator() *UUIDGen {
  return self.IdgenPtr
}

func (self *Context) Cache() *MemdClient {
  if self.CachePtr == nil {
    config := self.Config()
    self.CachePtr = NewMemdClient(config.Memcached.Servers...)
  }
  return self.CachePtr
}

func (ctx *Context) BucketApi() *BucketApi {
  if b := ctx.bucketapi; b != nil {
    return b
  }
  ctx.bucketapi = NewBucketApi(ctx)
  return ctx.bucketapi
}

func (ctx *Context) DeletedObjectApi() *DeletedObjectApi {
  if b := ctx.deletedobjectapi; b != nil {
    return b
  }
  ctx.deletedobjectapi = NewDeletedObjectApi(ctx)
  return ctx.deletedobjectapi
}

func (ctx *Context) EntityApi() *EntityApi {
  if b := ctx.entityapi; b != nil {
    return b
  }
  ctx.entityapi = NewEntityApi(ctx)
  return ctx.entityapi
}

func (ctx *Context) ObjectApi() *ObjectApi {
  if b := ctx.objectapi; b != nil {
    return b
  }
  ctx.objectapi = NewObjectApi(ctx)
  return ctx.objectapi
}

func (ctx *Context) QueueApi() QueueApiInterface {
  if b := ctx.queueapi; b != nil {
    return b
  }
  ctx.queueapi = NewQueueApi(ctx)
  return ctx.queueapi
}

func (ctx *Context) StorageApi() *StorageApi {
  if b := ctx.storageapi; b != nil {
    return b
  }
  ctx.storageapi = NewStorageApi(ctx)
  return ctx.storageapi
}

func (ctx *Context) StorageClusterApi() *StorageClusterApi {
  if b := ctx.storageclusterapi; b != nil {
    return b
  }
  ctx.storageclusterapi = NewStorageClusterApi(ctx)
  return ctx.storageclusterapi
}







