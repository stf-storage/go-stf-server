package api

import (
  "database/sql"
  "errors"
  "io"
  "log"
  "os"
  "strconv"
  "strings"
  "github.com/stf-storage/go-stf-server"
  "github.com/stf-storage/go-stf-server/cache"
  "github.com/stf-storage/go-stf-server/config"
  _ "github.com/go-sql-driver/mysql"
)

type TxnManager interface {
  TxnBegin() (func(), error)
  TxnCommit() error
  TxnRollback()
}

type ContextWithApi interface {
  Config() *config.Config
  Cache() *cache.MemdClient
  Txn() (*sql.Tx, error)
  LogMark(string, ...interface {}) func()
  Debugf(string, ...interface {})
  ApiHolder
  TxnManager
}

type Context struct {
  debug          *stf.DebugLog
  HomeStr         string
  bucketapi       *Bucket
  config          *config.Config
  deletedobjectapi *DeletedObject
  entityapi       *Entity
  objectapi       *Object
  queueapi        QueueApiInterface
  storageapi      *Storage
  storageclusterapi *StorageCluster
  cache           *cache.MemdClient
  maindb          *stf.DB
  tx              *sql.Tx
}

func (self *Context) Home() string { return self.HomeStr }
func (ctx *Context) LoadConfig() (*config.Config, error) {
  return config.LoadConfig(ctx.Home())
}

func NewContext(config *config.Config) *Context {
  return &Context{
    config: config,
    debug:  stf.NewDebugLog(),
  }
}

func (self *Context) NewScope() (*ScopedContext, error) {
  return NewScopedContext(self.Config())
}

type ScopedContext struct {
  config  *config.Config
  tx      *sql.Tx
  maindb  *stf.DB
}

func (self *Context) MainDB() (*stf.DB, error) {
  if db := self.maindb; db != nil {
    return db, nil
  }

  db, err := stf.ConnectDB(&self.Config().MainDB)
  if err != nil {
    return nil, err
  }

  self.maindb = db
  return db, nil
}

func (self *Context) Config() *config.Config {
  return self.config
}

func (self *ScopedContext) EndScope() {}
func NewScopedContext(config *config.Config) (*ScopedContext, error) {
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

var ErrNoTxnInProgress = errors.New("No transaction in progress")
func (self *Context) TxnCommit() error {
  if tx, _ := self.Txn(); tx != nil {
    if err := tx.Commit(); err != nil {
      return err
    }
    self.Debugf("Commited current transaction")
    self.tx = nil
    return nil
  }

  return ErrNoTxnInProgress
}

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
    ctx.debug = stf.NewDebugLog()
    ctx.debug.Prefix = "GLOBAL"
    ctx.debug.Printf("Starting debug log")
  }

  return ctx, nil
}

func (self *Context) Debugf (format string, args ...interface {}) {
  if dl := self.debug; dl != nil {
    dl.Printf(format, args...)
  }
}

func (self *Context) Cache() *cache.MemdClient {
  if self.cache == nil {
    config := self.Config()
    self.cache = cache.NewMemdClient(config.Memcached.Servers...)
  }
  return self.cache
}

func (ctx *Context) BucketApi() *Bucket {
  if b := ctx.bucketapi; b != nil {
    return b
  }
  ctx.bucketapi = NewBucket(ctx)
  return ctx.bucketapi
}

func (ctx *Context) DeletedObjectApi() *DeletedObject {
  if b := ctx.deletedobjectapi; b != nil {
    return b
  }
  ctx.deletedobjectapi = NewDeletedObject(ctx)
  return ctx.deletedobjectapi
}

func (ctx *Context) EntityApi() *Entity {
  if b := ctx.entityapi; b != nil {
    return b
  }
  ctx.entityapi = NewEntity(ctx)
  return ctx.entityapi
}

func (ctx *Context) ObjectApi() *Object {
  if b := ctx.objectapi; b != nil {
    return b
  }
  ctx.objectapi = NewObject(ctx)
  return ctx.objectapi
}

func (ctx *Context) QueueApi() QueueApiInterface {
  if b := ctx.queueapi; b != nil {
    return b
  }
  ctx.queueapi = NewQueue(ctx)
  return ctx.queueapi
}

func (ctx *Context) StorageApi() *Storage {
  if b := ctx.storageapi; b != nil {
    return b
  }
  ctx.storageapi = NewStorage(ctx)
  return ctx.storageapi
}

func (ctx *Context) StorageClusterApi() *StorageCluster {
  if b := ctx.storageclusterapi; b != nil {
    return b
  }
  ctx.storageclusterapi = NewStorageCluster(ctx)
  return ctx.storageclusterapi
}

func (ctx *Context) LogMark(format string, args ...interface {}) func() {
  if d := ctx.debug; d != nil {
    return d.LogMark(format, args...)
  }
  return func() {}
}

