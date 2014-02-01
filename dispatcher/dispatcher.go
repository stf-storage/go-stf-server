package dispatcher

import (
  "bytes"
  "database/sql"
  "errors"
  "fmt"
  "io/ioutil"
  "log"
  "math/rand"
  "net/http"
  "regexp"
  "runtime"
  "runtime/debug"
  "strings"
  "strconv"
  "github.com/braintree/manners"
  "github.com/stf-storage/go-stf-server"
  "github.com/lestrrat/go-apache-logformat"
  "github.com/lestrrat/go-file-rotatelogs"
  "github.com/lestrrat/go-server-starter-listener"
)

type Dispatcher struct {
  config          *stf.Config
  Address         string
  Ctx             *stf.Context
  ResponseWriter  *http.ResponseWriter
  Request         *http.Request
  logger          *apachelog.ApacheLog
  idgen           *UUIDGen
}

type DispatcherContext struct {
  *stf.Context
  ResponseWriter  http.ResponseWriter
  request         *http.Request
}

type DispatcherContextWithApi interface {
  stf.ContextWithApi
  Request() *http.Request
}

func New(config *stf.Config) *Dispatcher {
  d := &Dispatcher {
    config: config,
    idgen: NewIdGenerator(config.Dispatcher.ServerId),
  }

  d.logger = apachelog.CombinedLog.Clone()

  if filename := config.Dispatcher.AccessLog; filename != "" {
    rl := rotatelogs.NewRotateLogs(filename)
    if linkname := config.Dispatcher.AccessLogLink; linkname != "" {
      rl.LinkName = linkname
    }
    d.logger.SetOutput(rl)
  }

  return d
}

func (ctx *DispatcherContext) Request() *http.Request {
  return ctx.request
}

func (self *Dispatcher) IdGenerator() (*UUIDGen) {
  return self.idgen
}

func (self *Dispatcher) Start () {
  ctx := stf.NewContext(self.config)
  ncpu := runtime.NumCPU()
  nmaxprocs := runtime.GOMAXPROCS(-1)
  if ncpu != nmaxprocs {
    ctx.Debugf("Setting GOMAXPROCS to %d (was %d)", ncpu, nmaxprocs)
    runtime.GOMAXPROCS(ncpu)
  }

  // Work with Server::Stareter
  baseListener, err := ss.NewListenerOrDefault("tcp", self.config.Dispatcher.Listen)
  if err != nil {
    panic(fmt.Sprintf("Failed to listen at %s: %s", self.config.Dispatcher.Listen, err))
  }
  ctx.Debugf("Listening on %s", baseListener.Addr())

  s := manners.NewServer()
  l := manners.NewListener(baseListener, s)
  err = http.Serve(l, self)
  if err != nil {
    log.Fatal(
      fmt.Sprintf("Error from server's ListenAndServe: %s\n", err),
    )
  }
}

func (self *Dispatcher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
  ctx := &DispatcherContext{
    stf.NewContext(self.config),
    w,
    r,
  }
  closer := ctx.LogMark("[%s %s]", r.Method, r.URL.Path)
  defer closer()

  lw := apachelog.NewLoggingWriter(w, r, self.logger)
  defer lw.EmitLog()

  // Generic catch-all handler
  defer func() {
    if err := recover(); err != nil {
      debug.PrintStack()
      ctx.Debugf("Error while serving request: %s", err)
      http.Error(w, http.StatusText(500), 500)
    }
  } ()

  // First see if we have a proper URL that STF understands
  bucketName, objectName, err := parseObjectPath(r.URL.Path)
  if err != nil {
    http.Error(w, http.StatusText(404), 404)
    return
  }
  ctx.Debugf(
    "Parsed bucketName = '%s', objectName = '%s'\n",
    bucketName,
    objectName,
  )

  var resp *HTTPResponse
  switch r.Method {
  case "GET":
    resp = self.FetchObject(ctx, bucketName, objectName)
  case "DELETE":
    if objectName == "" {
      resp = self.DeleteBucket(ctx, bucketName)
    } else {
      resp = self.DeleteObject(ctx, bucketName, objectName)
    }
  case "PUT":
    // If the Content-Length is 0, then attempt to create the
    // Bucket Otherwise, try the bucket
    if cl := r.ContentLength; cl > 0 {
      resp = self.CreateObject(ctx, bucketName, objectName)
    } else {
      resp = self.CreateBucket(ctx, bucketName, objectName)
    }
  case "POST":
    resp = self.ModifyObject(ctx, bucketName, objectName)
  case "MOVE":
    dest := r.Header.Get("X-STF-Move-Destination")
    if objectName == "" {
      resp = self.RenameBucket(ctx, bucketName, dest)
    } else {
      resp = self.RenameObject(ctx, bucketName, objectName, dest)
    }
  default:
    resp = HTTPMethodNotAllowed
    return
  }

  if resp == nil {
    panic("Did not get a response object?!")
  }
  resp.Write(ctx.ResponseWriter)
}

func parseObjectPath(path string) (string, string, error) {
  precedingSlashRegexp := regexp.MustCompile(`^/`)

  // The path starts with a "/", but it's really not necessary
  path = precedingSlashRegexp.ReplaceAllLiteralString(path, "")

  len   := len(path)
  index := strings.Index(path, "/")

  if index == 0 {
    // Whoa, found a slash as the first thing?
    return "", "", errors.New("No bucket name could be extracted")
  }

  if index == -1 {
    // No slash? is this all bucket names?
    if len > 1 {
      return path, "", nil
    } else {
      return "", "", errors.New("No bucket name could be extracted")
    }
  }

  // If we got here, least 1 "/" was found.

  bucketName := path[0:index]
  objectName := path[index + 1:len]

  index = strings.Index(objectName, "/")
  if index == 0 {
    // Duplicate slashes. Fuck you
    return "", "", errors.New("Illegal object name")
  }

  return bucketName, objectName, nil
}

func (self *Dispatcher) CreateBucket(ctx *DispatcherContext, bucketName string, objectName string) *HTTPResponse {
  rollback, err := ctx.TxnBegin()
  if err != nil {
    ctx.Debugf("Failed to start transaction: %s", err)
    return HTTPInternalServerError
  }
  defer rollback()

  closer := ctx.LogMark("[Dispatcher.CreateBucket]")
  defer closer()

  if objectName != "" {
    return &HTTPResponse { Code: 400, Message: "Bad bucket name" }
  }

  bucketApi := ctx.BucketApi()

  id, err := bucketApi.LookupIdByName(bucketName)
  if err == nil { // No error, so we found a bucket
    ctx.Debugf("Bucket '%s' already exists (id = %d)", bucketName, id)
    return HTTPNoContent
  } else if err != sql.ErrNoRows {
    ctx.Debugf("Error while looking up bucket '%s': %s", bucketName, err)
    return HTTPInternalServerError
  }

  // If we got here, it's a new Bucket Create it
  id = self.IdGenerator().CreateId()
  log.Printf("id = %d", id)

  err = bucketApi.Create(
    id,
    bucketName,
  )

  if err != nil {
    ctx.Debugf("Failed to create bucket '%s': %s", bucketName, err)
    return HTTPInternalServerError
  }

  if err = ctx.TxnCommit(); err != nil {
    ctx.Debugf("Failed to commit: %s", err)
  }

  return HTTPCreated
}

func (self *Dispatcher) FetchObject(ctx DispatcherContextWithApi, bucketName string, objectName string) *HTTPResponse {
  lmc := ctx.LogMark("[Dispatcher.FetchObject]")
  defer lmc()

  rbc, err := ctx.TxnBegin()
  if err != nil {
    ctx.Debugf("%s", err)
    return HTTPInternalServerError
  }
  defer rbc()

  bucketApi := ctx.BucketApi()
  bucketId, err := bucketApi.LookupIdByName(bucketName)
  if err != nil {
    ctx.Debugf("Bucket %s does not exist", bucketName)
    return HTTPNotFound
  }
  bucketObj, err := bucketApi.Lookup(bucketId)
  if err != nil {
    return HTTPNotFound
  }

  objectApi := ctx.ObjectApi()
  objectId, err := objectApi.LookupIdByBucketAndPath(bucketObj, objectName)
  switch {
  case err == sql.ErrNoRows:
    // failed to lookup, 404
    return HTTPNotFound
  case err != nil:
    // Whatever error
    return HTTPInternalServerError
  }

  objectObj, err := objectApi.Lookup(objectId)
  if err != nil {
    return HTTPNotFound
  }

  ifModifiedSince := ctx.Request().Header.Get("If-Modified-Since")
  doHealthCheck := rand.Float64() < 0.001
  uri, err := objectApi.GetAnyValidEntityUrl(
    bucketObj,
    objectObj,
    doHealthCheck,
    ifModifiedSince,
  )

  switch {
  case uri == "":
    return HTTPNotFound
  case err == stf.ErrContentNotModified:
    // Special case
    return HTTPNotModified
  case err != nil:
    return HTTPInternalServerError
  }

  // something was found, return with a X-Reproxy-URL
  response := NewResponse(200)
  response.Header.Add("X-Reproxy-URL", uri)
  response.Header.Add("X-Accel-Redirect", "/redirect")

  if err = ctx.TxnCommit(); err != nil {
    ctx.Debugf("Failed to commit: %s", err)
  }

  return response
}

func (self *Dispatcher) DeleteObject (ctx stf.ContextWithApi, bucketName string, objectName string) *HTTPResponse {
  rollback, err := ctx.TxnBegin()
  if err != nil {
    ctx.Debugf("Failed to start transaction: %s", err)
    return HTTPInternalServerError
  }
  defer rollback()

  bucketApi := ctx.BucketApi()
  bucketId, err := bucketApi.LookupIdByName(bucketName)
  if err != nil {
    return &HTTPResponse { Code: 500, Message: "Failed to find bucket" }
  }

  bucketObj, err := bucketApi.Lookup(bucketId)
  if err != nil {
    return HTTPNotFound
  }

  if objectName == "" {
    return &HTTPResponse { Code: 500, Message: "Could not extact object name" }
  }

  objectApi := ctx.ObjectApi()
  objectId, err := objectApi.LookupIdByBucketAndPath(bucketObj, objectName)
  if err != nil {
    ctx.Debugf("Failed to lookup object %s/%s", bucketName, objectName)
    return HTTPNotFound
  }

  err = objectApi.MarkForDelete(objectId)
  if err != nil {
    ctx.Debugf("Failed to mark object (%d) as deleted: %s", objectId, err)
    return &HTTPResponse { Code : 500, Message: "Failed to mark object as deleted" }
  }

  err = ctx.TxnCommit()
  if err != nil {
    ctx.Debugf("Failed to commit: %s", err)
    return HTTPInternalServerError
  }

  ctx.Debugf("Successfully deleted object %s/%s", bucketName, objectName)
  go func () {
    queueApi := ctx.QueueApi()
    queueApi.Enqueue("queue_delete_object", strconv.FormatUint(objectId, 10))
  }()

  return HTTPNoContent
}

func (self *Dispatcher) DeleteBucket (ctx stf.ContextWithApi, bucketName string) *HTTPResponse {
  rollback, err := ctx.TxnBegin()
  if err != nil {
    ctx.Debugf("Failed to start transaction: %s", err)
    return HTTPInternalServerError
  }
  defer rollback()

  bucketApi := ctx.BucketApi()
  id, err := bucketApi.LookupIdByName(bucketName)

  if err != nil {
    return &HTTPResponse { Code: 500, Message: "Failed to find bucket" }
  }

  err = bucketApi.MarkForDelete(id)
  if err != nil {
    ctx.Debugf("Failed to delete bucket %s", err)
    return &HTTPResponse { Code: 500, Message: "Failed to delete bucket" }
  }

  if err = ctx.TxnCommit(); err != nil {
    ctx.Debugf("Failed to commit: %s", err)
  } else {
    ctx.Debugf("Deleted bucket '%s' (id = %d)", bucketName, id)
  }

  return HTTPNoContent
}

var reMatchSuffix = regexp.MustCompile(`\.([a-zA-Z0-9]+)$`)
func (self *Dispatcher) CreateObject (ctx DispatcherContextWithApi, bucketName string, objectName string) *HTTPResponse {
  lmc := ctx.LogMark("[Dispatcher.CreateObject]")
  defer lmc()

  rollback, err := ctx.TxnBegin()
  if err != nil {
    ctx.Debugf("Failed to start transaction: %s", err)
    return HTTPInternalServerError
  }
  defer rollback()

  bucketApi := ctx.BucketApi()
  bucketId, err := bucketApi.LookupIdByName(bucketName)
  if err != nil {
    return &HTTPResponse { Code: 500, Message: "Failed to find bucket" }
  }

  bucketObj, err := bucketApi.Lookup(bucketId)
  if err != nil {
    return HTTPNotFound
  }

  if objectName == "" {
    return &HTTPResponse { Code: 500, Message: "Could not extact object name" }
  }

  objectApi := ctx.ObjectApi()
  oldObjectId, err := objectApi.LookupIdByBucketAndPath(bucketObj, objectName)
  switch {
  case err == sql.ErrNoRows:
    // Just means that this is a new object
  case err != nil:
    // Some unknown error occurred
    return HTTPInternalServerError
  default:
    // Found oldObjectId. Mark this old object to be deleted
    // Note: Don't send to the queue just yet
    ctx.Debugf(
      "Object '%s' on bucket '%s' already exists",
      objectName,
      bucketName,
    )
    objectApi.MarkForDelete(oldObjectId)
  }

  matches := reMatchSuffix.FindStringSubmatch(ctx.Request().URL.Path)
  var suffix string
  if len(matches) < 2 {
    suffix = "dat"
  } else {
    suffix = matches[1]
  }

  objectId := self.IdGenerator().CreateId()

  // XXX Request.Body is an io.ReadCloser, which doesn't implment
  // a Seek() mechanism. I don't know if there's a better machanism
  // for this, but because we want to be using the body many times
  // we create a new Buffer
  // XXX Do we need to check for malicious requests where
  // ContentLength != Request.Body length?

  body, err := ioutil.ReadAll(ctx.Request().Body)
  if err != nil {
    ctx.Debugf("Failed to read request body: %s", err)
    return HTTPInternalServerError
  }

  buf := bytes.NewReader(body)

  err = objectApi.Store(
    objectId,
    bucketObj,
    objectName,
    ctx.Request().ContentLength,
    buf,
    suffix,
    false, // isRepair = false
    true,  // force = true
  )

  if err != nil {
    return HTTPInternalServerError
  }

  ctx.Debugf("Commiting changes")
  err = ctx.TxnCommit()
  if err != nil {
    ctx.Debugf("Failed to commit transaction: %s", err)
    return HTTPInternalServerError
  }

  ctx.Debugf("Successfully created object %s/%s", bucketName, objectName)
  go func () {
    queueApi := ctx.QueueApi()
    queueApi.Enqueue("queue_replicate", strconv.FormatUint(objectId, 10))
  }()
  return HTTPCreated
}

func (self *Dispatcher) ModifyObject (ctx stf.ContextWithApi, bucketName string, objectName string) *HTTPResponse {
  return nil
}

// MOVE /bucket_name
// X-STF-Move-Destination: /new_name
func (self *Dispatcher) RenameBucket (ctx stf.ContextWithApi, bucketName string, dest string) *HTTPResponse {
  return nil
}

// MOVE /bucket_name
// X-STF-Move-Destination: /new_name
func (self *Dispatcher) RenameObject(ctx stf.ContextWithApi, bucketName string, objectName string, dest string) *HTTPResponse {
  return nil
}
