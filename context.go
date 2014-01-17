package stf

import (
  "database/sql"
  "errors"
  "fmt"
  "io"
  "log"
  "math/rand"
  "net/http"
  "os"
  "strconv"
  "strings"
  "time"
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

type DebugWriter interface {
  DebugLog()    *DebugLog
  Debugf(string, ...interface{})
  LogMark(string, ...interface{}) func()
}

type Context interface {
  Config()      *Config
  MainDB()      (*sql.DB, error)

  Cache()       (*MemdClient)

  Txn()         (*sql.Tx, error)
  TxnBegin()    (func(), error)
  TxnCommit()   error
  TxnRollback() error
  SetTxn(*sql.Tx)
  SetTxnCommited(bool)

  DebugWriter
}

type ContextWithApi interface {
  Context
  ApiHolder
}

type BaseContext struct {
  DebugLogPtr     *DebugLog
  TxnPtr          *sql.Tx
  TxnCommited     bool
}

type GlobalContext struct {
  BaseContext
  HomeStr         string
  ConfigPtr       *Config
  CachePtr        *MemdClient
  MainDBPtr       *sql.DB
  IdgenPtr        *UUIDGen
}

type LocalContext struct {
  BaseContext
  BucketApiPtr         *BucketApi
  DeletedObjectApiPtr  *DeletedObjectApi
  EntityApiPtr         *EntityApi
  ObjectApiPtr         *ObjectApi
  QueueApiPtr          QueueApiInterface
  StorageApiPtr        *StorageApi
  StorageClusterApiPtr *StorageClusterApi
  GlobalContextPtr     Context
}

type RequestContext struct {
  LocalContext
  Request         *http.Request
  ResponseWriter  http.ResponseWriter
}

// BaseContext
func (self *BaseContext) DebugLog()   *DebugLog { return self.DebugLogPtr }

func (self *BaseContext) Debugf(format string, args ...interface {}) {
  if dl := self.DebugLog(); dl != nil {
    dl.Printf(format, args...)
  }
}

func (self *BaseContext) LogMark(format string, args ...interface{}) func () {
  dbgLog := self.DebugLog()
  if dbgLog == nil {
    return func() {}
  }

  marker := fmt.Sprintf(format, args...)

  self.Debugf("%s START", marker)
  closer := self.DebugLog().LogIndent()
  return func () {
    err := recover()
    if err != nil {
      self.Debugf("Encoundered panic during '%s': %s", marker, err)
    }
    closer()
    self.Debugf("%s END", marker)
    if err != nil {
      panic(err)
    }
  }
}

func (self *GlobalContext) Home() string { return self.HomeStr }
func (ctx *GlobalContext) LoadConfig() (*Config, error) {
  return LoadConfig(ctx.Home())
}

func NewContext() (*GlobalContext, error) {
  rand.Seed(time.Now().UTC().UnixNano())

  home := GetHome()
  ctx := &GlobalContext{
    HomeStr: home,
  }
  return ctx, nil
}

func BootstrapContext() (*GlobalContext, error) {
  ctx, err  := NewContext()
  if err != nil {
    return nil, err
  }

  cfg, err  := ctx.LoadConfig()
  if err != nil {
    return nil, err
  }

  ctx.ConfigPtr = cfg
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

func (self *GlobalContext) DebugLog() *DebugLog {
  return self.DebugLogPtr
}

func (self *GlobalContext) Debugf (format string, args ...interface {}) {
  if dl := self.DebugLog(); dl != nil {
    dl.Printf(format, args...)
  }
}

func (self *GlobalContext) Config() *Config { return self.ConfigPtr }

func (self *GlobalContext) MainDB() (*sql.DB, error) {
  if self.MainDBPtr == nil {
    db, err := ConnectDB(&self.Config().MainDB)
    if err != nil {
      return nil, err
    }
    self.MainDBPtr = db
  }
  return self.MainDBPtr, nil
}

func (self *RequestContext) MainDB() (*sql.DB, error) {
  return self.GlobalContext().MainDB()
}

func (self *GlobalContext) IdGenerator() *UUIDGen {
  return self.IdgenPtr
}

func (self *GlobalContext) Cache() *MemdClient {
  if self.CachePtr == nil {
    config := self.Config()
    self.CachePtr = NewMemdClient(config.Memcached.Servers...)
  }
  return self.CachePtr
}

func (self *GlobalContext) NewRequestContext(w http.ResponseWriter, r *http.Request) *RequestContext {
  rc := &RequestContext {
    LocalContext { GlobalContextPtr: self },
    r,
    w,
  }

  config := self.Config()
  if config.Global.Debug {
    rc.DebugLogPtr = NewDebugLog()
    rc.DebugLogPtr.Prefix = GenerateRandomId(fmt.Sprintf("%p", rc), 8)
  }
  return rc
}

func (self *RequestContext) GlobalContext() Context {
  return self.GlobalContextPtr
}

func (self *RequestContext) Config() *Config {
  return self.GlobalContext().Config()
}

func (self *RequestContext) Cache() *MemdClient {
  return self.GlobalContext().Cache()
}

func (self *RequestContext) IdGenerator() *UUIDGen {
  return self.GlobalContext().(*GlobalContext).IdGenerator()
}

func (self *RequestContext) BucketApi() *BucketApi {
  if self.BucketApiPtr == nil {
    self.BucketApiPtr = NewBucketApi(self)
  }
  return self.BucketApiPtr
}

func (self *RequestContext) EntityApi() *EntityApi {
  if self.EntityApiPtr == nil {
    self.EntityApiPtr = NewEntityApi(self)
  }
  return self.EntityApiPtr
}

func (self *RequestContext) DeletedObjectApi() *DeletedObjectApi {
  if self.DeletedObjectApiPtr == nil {
    self.DeletedObjectApiPtr = NewDeletedObjectApi(self)
  }
  return self.DeletedObjectApiPtr
}

func (self *RequestContext) ObjectApi() *ObjectApi {
  if self.ObjectApiPtr == nil {
    self.ObjectApiPtr = NewObjectApi(self)
  }
  return self.ObjectApiPtr
}

func (self *RequestContext) QueueApi() QueueApiInterface {
  if self.QueueApiPtr == nil {
    self.QueueApiPtr = NewQueueApi(self)
  }
  return self.QueueApiPtr
}

func (self *RequestContext) StorageApi() *StorageApi {
  if self.StorageApiPtr == nil {
    self.StorageApiPtr = NewStorageApi(self)
  }
  return self.StorageApiPtr
}

func (self *RequestContext) StorageClusterApi() *StorageClusterApi {
  if self.StorageClusterApiPtr == nil {
    self.StorageClusterApiPtr = NewStorageClusterApi(self)
  }
  return self.StorageClusterApiPtr
}

func (self *GlobalContext) TxnBegin () (func(), error) {
  return TxnBeginWith(self)
}

func (self *RequestContext) TxnBegin() (func(), error) {
  return TxnBeginWith(self)
}

var ErrNoTxnInProgress = errors.New("No transaction in progress")
func (self *BaseContext) Txn() (*sql.Tx, error) {
  if self.TxnPtr == nil {
    return nil, ErrNoTxnInProgress
  }
  return self.TxnPtr, nil
}

func (self *BaseContext) SetTxn(tx *sql.Tx) {
  self.TxnPtr = tx
}

func (self *BaseContext) SetTxnCommited(x bool) {
  self.TxnCommited = x
}

var ErrTxnAlreadyStarted = errors.New("There's already a transaction being processed")
func TxnBeginWith(ctx Context) (func(), error) {
  txn, err := ctx.Txn()
  if err == nil {
    return nil, ErrTxnAlreadyStarted
  }
  ctx.Debugf("Starting new transaction")

  db, err := ctx.MainDB()
  if err != nil {
    return nil, err
  }

  txn, err = db.Begin()
  if err != nil {
    ctx.Debugf("Failed to start transaction: %s", err)
    return nil, err
  }

  ctx.SetTxn(txn)
  ctx.SetTxnCommited(false)

  return func () { ctx.TxnRollback() }, nil
}

func (self *BaseContext) TxnCommit() error {
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

func (self *BaseContext) TxnRollback() error {
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

func (self *GlobalContext) Destroy() {
  self.ConfigPtr = nil
}

func (self *RequestContext) Destroy() {
  self.TxnRollback()
}

