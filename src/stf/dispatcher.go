package stf

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
  "strings"
  "strconv"
)

type Dispatcher struct {
  Address string
  Ctx            *GlobalContext
  ResponseWriter *http.ResponseWriter
  Request        *http.Request
}

func BootstrapDispatcher(ctx *GlobalContext) (*Dispatcher, error) {
  cfg := (*ctx.Config()).Dispatcher
  return NewDispatcher(ctx, cfg.ServerId, &cfg.Listen), nil
}

func NewDispatcher(ctx *GlobalContext, id uint64, addr *string) *Dispatcher {
  d := new(Dispatcher)
  if addr != nil {
    d.Address = *addr
  } else {
    d.Address = ":8080"
  }
  d.Ctx = ctx
  return d
}

func (self *Dispatcher) Debugf (format string, args ...interface {}) {
  self.Ctx.Debugf(format, args...)
}

func (self *Dispatcher) Start () {
  self.Debugf("Starting server at %s\n", self.Address)
  server    := &http.Server{
    Addr:     self.Address,
    Handler:  self,
  }

  err := server.ListenAndServe()
  if err != nil {
    log.Fatal(
      fmt.Sprintf("Error from server's ListenAndServe: %s\n", err),
    )
  }
}

func (self *Dispatcher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
  ctx := self.Ctx.NewRequestContext(w, r)
  defer ctx.Destroy()

  closer := ctx.LogMark("[%s %s]", r.Method, r.URL.Path)
  defer closer()

  // Generic catch-all handler
  defer func() {
    if err := recover(); err != nil {
      self.Debugf("Error while serving request: %s", err)
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

func (self *Dispatcher) CreateBucket(ctx *RequestContext, bucketName string, objectName string) *HTTPResponse {
  ctx.TxnBegin()
  defer ctx.TxnRollback()

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
  id = ctx.IdGenerator().CreateId()
  log.Printf("id = %d", id)

  err = bucketApi.Create(
    id,
    bucketName,
  )

  if err != nil {
    self.Debugf("Failed to create bucket '%s': %s", bucketName, err)
    return HTTPInternalServerError
  }

  ctx.TxnCommit()

  return HTTPCreated
}

func (self *Dispatcher) FetchObject(ctx *RequestContext, bucketName string, objectName string) *HTTPResponse {
  ctx.TxnBegin()
  defer ctx.TxnRollback()

  bucketApi := ctx.BucketApi()
  bucketId, err := bucketApi.LookupIdByName(bucketName)
  if err != nil {
    self.Debugf("Bucket %s does not exist", bucketName)
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

  ifModifiedSince := ctx.Request.Header.Get("If-Modified-Since")
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
  case err == ErrContentNotModified:
    // Special case
    return HTTPNotModified
  case err != nil:
    return HTTPInternalServerError
  }

  // something was found, return with a X-Reproxy-URL
  response := NewResponse(200)
  response.Header.Add("X-Reproxy-URL", uri)
  response.Header.Add("X-Accel-Redirect", "/redirect")

  ctx.TxnCommit()

  return response
}

func (self *Dispatcher) DeleteObject (ctx *RequestContext, bucketName string, objectName string) *HTTPResponse {
  ctx.TxnBegin()
  defer ctx.TxnRollback()

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
    self.Debugf("Failed to mark object (%d) as deleted: %s", objectId, err)
    return &HTTPResponse { Code : 500, Message: "Failed to mark object as deleted" }
  }

  err = QueueInsert(ctx, "delete_object", strconv.FormatUint(objectId, 10))
  if err != nil {
    self.Debugf("Failed to send object (%d) to delete_object queue: %s", objectId, err)
    return &HTTPResponse { Code : 500, Message: "Failed to delete object" }
  }

  return nil
}

func (self *Dispatcher) DeleteBucket (ctx *RequestContext, bucketName string) *HTTPResponse {
  bucketApi := ctx.BucketApi()
  id, err := bucketApi.LookupIdByName(bucketName)

  if err != nil {
    return &HTTPResponse { Code: 500, Message: "Failed to find bucket" }
  }

  err = bucketApi.MarkForDelete(id)
  if err != nil {
    self.Debugf("Failed to delete bucket %s", err)
    return &HTTPResponse { Code: 500, Message: "Failed to delete bucket" }
  }

  self.Debugf("Deleted bucket '%s' (id = %d)", bucketName, id)

  return HTTPNoContent
}

var reMatchSuffix = regexp.MustCompile(`\.([a-zA-Z0-9]+)$`)
func (self *Dispatcher) CreateObject (ctx *RequestContext, bucketName string, objectName string) *HTTPResponse {

  ctx.TxnBegin()
  defer ctx.TxnRollback()

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

  matches := reMatchSuffix.FindStringSubmatch(ctx.Request.URL.Path)
  var suffix string
  if len(matches) < 2 {
    suffix = "dat"
  } else {
    suffix = matches[1]
  }

  objectId := ctx.IdGenerator().CreateId()

  // XXX Request.Body is an io.ReadCloser, which doesn't implment
  // a Seek() mechanism. I don't know if there's a better machanism
  // for this, but because we want to be using the body many times
  // we create a new Buffer
  // XXX Do we need to check for malicious requests where
  // ContentLength != Request.Body length?

  body, err := ioutil.ReadAll(ctx.Request.Body)
  if err != nil {
    ctx.Debugf("Failed to read request body: %s", err)
    return HTTPInternalServerError
  }

  buf := bytes.NewReader(body)

  err = objectApi.Store(
    objectId,
    bucketId,
    objectName,
    ctx.Request.ContentLength,
    buf,
    suffix,
    false, // isRepair = false
    true,  // force = true
  )

  if err != nil {
    return HTTPInternalServerError
  }

  ctx.Debugf("Successfully created object %s/%s", bucketName, objectName)
  ctx.TxnCommit()

  return HTTPCreated
}
func (self *Dispatcher) ModifyObject (ctx *RequestContext, bucketName string, objectName string) *HTTPResponse {
  return nil
}