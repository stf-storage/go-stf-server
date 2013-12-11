package stf

import (
  "crypto/sha1"
  "database/sql"
  "errors"
  "fmt"
  "io"
  "log"
  "math/rand"
  "net/http"
  "os"
  "path"
  "path/filepath"
  "time"
  "strconv"
  "strings"
  "code.google.com/p/gcfg"
  _ "github.com/go-sql-driver/mysql"
)

type DebugLog bool

func (d DebugLog) Printf(format string, args ...interface{}) {
  if d {
    log.Printf(format, args...)
  }
}

type Context interface {
  Config()      *Config
  MainDB()      (*sql.DB, error)
  QueueDB(int)  (*sql.DB, error)
  DebugLog()    DebugLog
}

type TxnHolder interface {
  Txn()         (*sql.Tx, error)
  TxnBegin()    (*sql.Tx, error)
  TxnCommit()   error
  TxnRollback() error
}

type GlobalContext struct {
  config    *Config
  home      string
  cache     *MemdClient
  mainDB    *sql.DB
  numQueueDB int
  queueDB   []*sql.DB
  debugLog  DebugLog
  idgen     UUIDGen
}

type RequestContext struct {
  bucketApi         *BucketApi
  entityApi         *EntityApi
  objectApi         *ObjectApi
  queueApi          *QueueApi
  storageApi        *StorageApi
  storageClusterApi *StorageClusterApi
  DebugLog        DebugLog
  globalContext   *GlobalContext
  Id              string
  Indent          string
  Request         *http.Request
  ResponseWriter  http.ResponseWriter
  txn             *sql.Tx
  txnCommited     bool
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

  ctx.config = cfg
  ctx.debugLog = DebugLog((*cfg).Global.Debug)
  ctx.numQueueDB = len(cfg.QueueDB)

  return ctx, nil
}

func (self *GlobalContext) Debugf (format string, args ...interface {}) {
  self.debugLog.Printf(format, args...)
}

func (self *GlobalContext) Home() string { return self.home }
func (self *GlobalContext) Config() *Config { return self.config }

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

func (self *RequestContext) QueueDB(i int) (*sql.DB, error) {
  return self.globalContext.QueueDB(i)
}

func (self *RequestContext) NumQueueDB() int {
  return self.globalContext.numQueueDB
}

func (self *GlobalContext) IdGenerator() *UUIDGen {
  return &self.idgen
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
    globalContext: self,
    Request: r,
    ResponseWriter: w,
    Indent: "",
  }

  h := sha1.New()
  io.WriteString(h, fmt.Sprintf("%p", rc))
  io.WriteString(h, strconv.FormatInt(time.Now().UTC().UnixNano(), 10))
  rc.Id = (fmt.Sprintf("%x", h.Sum(nil)))[0:8]
  return rc
}

const INDENT_PATTERN string = "  "
func (self *RequestContext) LogIndent() func() {
  self.Indent = INDENT_PATTERN + self.Indent
  return func () {
    self.Indent = strings.TrimPrefix(self.Indent, INDENT_PATTERN)
  }
}

func (self *RequestContext) Debugf(format string, args ...interface {}) {
  message := fmt.Sprintf(format, args...)
  self.globalContext.Debugf("%s %s %s", self.Id, self.Indent, message)
}

func (self *RequestContext) LogMark(format string, args ...interface{}) func () {
  marker := fmt.Sprintf(format, args...)

  self.Debugf("%s START", marker)
  iCloser := self.LogIndent()
  return func () {
    err := recover()
    if err != nil {
      self.Debugf("Encoundered panic during '%s': %s", marker, err)
    }
    iCloser()
    self.Debugf("%s END", marker)
    if err != nil {
      panic(err)
    }
  }
}

func (self *RequestContext) Cache() *MemdClient {
  return self.globalContext.Cache()
}

func (self *RequestContext) IdGenerator() *UUIDGen {
  return self.globalContext.IdGenerator()
}

func (self *RequestContext) BucketApi() *BucketApi {
  if self.bucketApi == nil {
    self.bucketApi = NewBucketApi(self)
  }
  return self.bucketApi
}

func (self *RequestContext) EntityApi() *EntityApi {
  if self.entityApi == nil {
    self.entityApi = NewEntityApi(self)
  }
  return self.entityApi
}

func (self *RequestContext) ObjectApi() *ObjectApi {
  if self.objectApi == nil {
    self.objectApi = NewObjectApi(self)
  }
  return self.objectApi
}

func (self *RequestContext) QueueApi() *QueueApi {
  if self.queueApi == nil {
    self.queueApi = NewQueueApi(self)
  }
  return self.queueApi
}

func (self *RequestContext) StorageApi() *StorageApi {
  if self.storageApi == nil {
    self.storageApi = NewStorageApi(self)
  }
  return self.storageApi
}

func (self *RequestContext) StorageClusterApi() *StorageClusterApi {
  if self.storageClusterApi == nil {
    self.storageClusterApi = NewStorageClusterApi(self)
  }
  return self.storageClusterApi
}

func (self *RequestContext) Txn() (*sql.Tx, error) {
  if self.txn == nil {
    _, err := self.TxnBegin()
    if err != nil {
      return nil, errors.New(
        fmt.Sprintf(
          "Failed to begin transaction: %s",
          err,
        ),
      )
    }
  }
  return self.txn, nil
}

func (self *RequestContext) TxnBegin() (*sql.Tx, error) {
  // What, there's an existing transaction?!
  if self.txn != nil {
    return nil, errors.New("There's already a transaction being processed")
  }

  self.Debugf("Starting new transaction")
  db, err := self.globalContext.MainDB()
  if err != nil {
    return nil, err
  }
  txn, err := db.Begin()

  if err != nil {
    self.Debugf("Failed to start transaction: %s", err)
    return nil, err
  }

  self.txn = txn
  self.txnCommited = false
  return txn, nil
}

func (self *RequestContext) TxnCommit() error {
  txn := self.txn
  if txn != nil {
    self.Debugf("Committing transaction")
    err := txn.Commit()
    if err != nil {
      return err
    }

    self.Debugf("Transaction commited")
    self.txnCommited = true
    self.txn = nil
  }
  return nil
}

func (self *RequestContext) TxnRollback() error {
  txn := self.txn
  if txn == nil {
    return nil
  }

  if self.txnCommited {
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
  self.config     = nil
}

func (self *RequestContext) Destroy() {
  self.TxnRollback()
}

