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
  "code.google.com/p/gcfg"
  "strconv"
  "strings"
  _ "github.com/go-sql-driver/mysql"
)

type DebugLog bool

func (d DebugLog) Printf(format string, args ...interface{}) {
  if d {
    log.Printf(format, args...)
  }
}

type GlobalContext struct {
  config    *Config
  home      string
  cache     *MemdClient
  mainDB    *sql.DB
  queueDB   []*sql.DB
  debugLog  DebugLog
  idgen     UUIDGen
}

type RequestContext struct {
  bucketApi         *BucketApi
  entityApi         *EntityApi
  objectApi         *ObjectApi
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

  return ctx, nil
}

func (self *GlobalContext) Debugf (format string, args ...interface {}) {
  self.debugLog.Printf(format, args...)
}

func (self *GlobalContext) Home() string { return self.home }
func (self *GlobalContext) Config() *Config { return self.config }

func (self *GlobalContext) connectDB (config DatabaseConfig) *sql.DB {
  if config.Dbtype == "" {
    config.Dbtype = "mysql"
  }

  if config.Hostname == "" {
    config.Hostname = "127.0.0.1"
  }

  if config.Port <= 0 {
    config.Port = 3306
  }

  if config.Dbname == "" {
    config.Dbname = "stf"
  }

  if config.Protocol == "" {
    config.Protocol = "tcp"
  }

  dsn := fmt.Sprintf(
    "%s:%s@%s(%s:%d)/%s?parseTime=true",
    config.Username,
    config.Password,
    config.Protocol,
    config.Hostname,
    config.Port,
    config.Dbname,
  )

  self.Debugf("Connecting to dsn %s", dsn)

  db, err := sql.Open(config.Dbtype, dsn)

  if err != nil {
    log.Fatalf("Failed to connect to database: %s", err)
  }

  return db
}

func (self *GlobalContext) MainDB() *sql.DB {
  if self.mainDB == nil {
    self.mainDB = self.connectDB(self.Config().MainDB)
  }
  return self.mainDB
}

// Gets the i-th Queue DB
func (self *GlobalContext) QueueDB(i int) *sql.DB {
  if self.queueDB[i] == nil {
    config := *self.Config()
    self.queueDB[i] = self.connectDB(config.QueueDB[i])
  }
  return self.queueDB[i]
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

func (self *RequestContext) Txn() *sql.Tx {
  if self.txn == nil {
    _, err := self.TxnBegin()
    if err != nil {
      log.Fatalf("Failed to begin transaction: %s", err)
    }
  }
  return self.txn
}

func (self *RequestContext) TxnBegin() (*sql.Tx, error) {
  // What, there's an existing transaction?!
  if self.txn != nil {
    return nil, errors.New("There's already a transaction being processed")
  }

  self.Debugf("Starting new transaction")
  txn, err := self.globalContext.MainDB().Begin()

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

