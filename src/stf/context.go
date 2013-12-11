package stf

import (
  "crypto/sha1"
  "database/sql"
  "errors"
  "fmt"
  "io"
  "math/rand"
  "net/http"
  "os"
  "path"
  "path/filepath"
  "strconv"
  "time"
  "code.google.com/p/gcfg"
  _ "github.com/go-sql-driver/mysql"
)

type ApiHolder interface {
  BucketApi()   *BucketApi
  EntityApi()   *EntityApi
  ObjectApi()   *ObjectApi
  QueueApi()    *QueueApi
  StorageApi()  *StorageApi
  StorageClusterApi() *StorageClusterApi
}

type Context interface {
  Config()      *Config
  MainDB()      (*sql.DB, error)
  QueueDB(int)  (*sql.DB, error)
  NumQueueDB()  int

  Cache()       (*MemdClient)

  Txn()         (*sql.Tx, error)
  TxnBegin()    (func(), error)
  TxnCommit()   error
  TxnRollback() error
  SetTxn(*sql.Tx)
  SetTxnCommited(bool)

  DebugLog()    *DebugLog
  Debugf(string, ...interface{})
  LogMark(string, ...interface{}) func()
}

type ContextWithApi interface {
  Context
  ApiHolder
}

type BaseContext struct {
  DebugLogPtr     *DebugLog
  NumQueueDBCount int
  TxnPtr          *sql.Tx
  TxnCommited     bool
}

type GlobalContext struct {
  BaseContext
  ConfigPtr *Config
  home      string
  cache     *MemdClient
  mainDB    *sql.DB
  queueDB   []*sql.DB
  IdgenPtr      *UUIDGen
}

type LocalContext struct {
  BaseContext
  BucketApiPtr         *BucketApi
  EntityApiPtr         *EntityApi
  ObjectApiPtr         *ObjectApi
  QueueApiPtr          *QueueApi
  StorageApiPtr        *StorageApi
  StorageClusterApiPtr *StorageClusterApi
  GlobalContextPtr     Context
}

type RequestContext struct {
  *LocalContext
  Request         *http.Request
  ResponseWriter  http.ResponseWriter
}

// BaseContext
func (self *BaseContext) NumQueueDB() int {
  return self.NumQueueDBCount
}

func (self *BaseContext) DebugLog() *DebugLog {
  return self.DebugLogPtr
}

func (self *BaseContext) Debugf(format string, args ...interface {}) {
  if dl := self.DebugLog(); dl != nil {
    dl.Printf(format, args...)
  }
}

func (self *BaseContext) LogMark(format string, args ...interface{}) func () {
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

func (ctx *GlobalContext) NewConfig () (*Config, error) {
  cfg   := &Config {}

  file  := os.Getenv("STF_CONFIG")
  if file == "" {
    file = path.Join("etc", "config.gcfg")
  }
  if ! filepath.IsAbs(file) {
    file = path.Join(ctx.Home(), file)
  }

  err := gcfg.ReadFileInto(cfg, file)
  if err != nil {
    return nil, err
  }
  return cfg, nil
}

func NewContext() (*GlobalContext, error) {
  rand.Seed(time.Now().UTC().UnixNano())

  ctx := &GlobalContext{}
  home, err := os.Getwd()
  if err != nil {
    return nil, err
  }
  ctx.home = home
  return ctx, nil
}

func BootstrapContext() (*GlobalContext, error) {
  ctx, err  := NewContext()
  if err != nil {
    return nil, err
  }

  cfg, err  := ctx.NewConfig()
  if err != nil {
    return nil, err
  }

  ctx.ConfigPtr = cfg
  ctx.NumQueueDBCount = len(cfg.QueueDB)

  if cfg.Global.Debug {
    ctx.DebugLogPtr = NewDebugLog()
    ctx.DebugLogPtr.Prefix = "GLOBAL"
  }

  return ctx, nil
}

func (self *GlobalContext) DebugLog() *DebugLog {
  return self.DebugLogPtr
}

func (self *GlobalContext) Debugf (format string, args ...interface {}) {
  if dl := self.DebugLog(); dl != nil {
    self.DebugLogPtr.Printf(format, args...)
  }
}

func (self *GlobalContext) Home() string { return self.home }
func (self *GlobalContext) Config() *Config { return self.ConfigPtr }

func (self *GlobalContext) connectDB (config DatabaseConfig) (*sql.DB, error) {
  if config.Dbtype == "" {
    config.Dbtype = "mysql"
  }

  if config.ConnectString == "" {
    switch config.Dbtype {
    case "mysql":
      config.ConnectString = "tcp(127.0.0.1:3306)"
    default:
      return nil, errors.New(
        fmt.Sprintf(
          "No database connect string provided, and can't assign a default value for dbtype '%s'",
          config.Dbtype,
        ),
      )
    }
  }

  if config.Dbname == "" {
    config.Dbname = "stf"
  }

  dsn := fmt.Sprintf(
    "%s:%s@%s/%s?parseTime=true",
    config.Username,
    config.Password,
    config.ConnectString,
    config.Dbname,
  )

  self.Debugf("Connecting to dsn %s", dsn)

  db, err := sql.Open(config.Dbtype, dsn)

  if err != nil {
    return nil, errors.New(
      fmt.Sprintf("Failed to connect to database: %s", err),
    )
  }

  return db, nil
}

func (self *GlobalContext) MainDB() (*sql.DB, error) {
  if self.mainDB == nil {
    db, err := self.connectDB(self.Config().MainDB)
    if err != nil {
      return nil, err
    }
    self.mainDB = db
  }
  return self.mainDB, nil
}

// Gets the i-th Queue DB
func (self *GlobalContext) QueueDB(i int) (*sql.DB, error) {
  if self.queueDB[i] == nil {
    config := self.Config().QueueDB[i]
    db, err := self.connectDB(config)
    if err != nil {
      return nil, err
    }
    self.queueDB[i] = db
  }
  return self.queueDB[i], nil
}

func (self *RequestContext) MainDB() (*sql.DB, error) {
  return self.GlobalContext().MainDB()
}

func (self *RequestContext) QueueDB(i int) (*sql.DB, error) {
  return self.GlobalContext().QueueDB(i)
}

func (self *RequestContext) NumQueueDB() int {
  return self.GlobalContext().NumQueueDB()
}

func (self *GlobalContext) IdGenerator() *UUIDGen {
  return self.IdgenPtr
}

func (self *GlobalContext) Cache() *MemdClient {
  if self.cache == nil {
    config := *self.Config()
    self.cache = NewMemdClient(config.Memcached.Servers...)
  }
  return self.cache
}

func (self *GlobalContext) NewRequestContext(w http.ResponseWriter, r *http.Request) *RequestContext {
  rc := &RequestContext {
    &LocalContext { GlobalContextPtr: self },
    r,
    w,
  }

  config := self.Config()
  if config.Global.Debug {
    rc.DebugLogPtr = NewDebugLog()
    h := sha1.New()
    io.WriteString(h, fmt.Sprintf("%p", rc))
    io.WriteString(h, strconv.FormatInt(time.Now().UTC().UnixNano(), 10))
    rc.DebugLogPtr.Prefix = (fmt.Sprintf("%x", h.Sum(nil)))[0:8]
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

func (self *RequestContext) ObjectApi() *ObjectApi {
  if self.ObjectApiPtr == nil {
    self.ObjectApiPtr = NewObjectApi(self)
  }
  return self.ObjectApiPtr
}

func (self *RequestContext) QueueApi() *QueueApi {
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

