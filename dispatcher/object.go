package dispatcher

import (
  "bytes"
  "database/sql"
  "io/ioutil"
  "math/rand"
  "regexp"
  "strconv"
  "github.com/stf-storage/go-stf-server/api"
)

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
    ctx.Debugf("No entry found in database")
    // failed to lookup, 404
    return HTTPNotFound
  case err != nil:
    // Whatever error
    ctx.Debugf("Errored during lookup: %s", err)
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
  case err == api.ErrContentNotModified:
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

func (self *Dispatcher) DeleteObject (ctx api.ContextWithApi, bucketName string, objectName string) *HTTPResponse {
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

func (self *Dispatcher) ModifyObject (ctx api.ContextWithApi, bucketName string, objectName string) *HTTPResponse {
  return nil
}

// MOVE /bucket_name
// X-STF-Move-Destination: /new_name
func (self *Dispatcher) RenameObject(ctx api.ContextWithApi, bucketName string, objectName string, dest string) *HTTPResponse {
  return nil
}


