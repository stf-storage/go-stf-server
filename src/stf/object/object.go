package object

import (
  "bytes"
  "database/sql"
  "errors"
  "fmt"
  "log"
  "net/http"
  "stf"
  "stf/context"
  "stf/cluster"
  "stf/queue"
  "stf/storage"
  "strconv"
  "strings"
  randbo "github.com/dustin/randbo"
)

var ErrContentNotModified error = errors.New("Request Content Not Modified")

func LookupIdByBucketAndPath(ctx *context.RequestContext, bucketObj *stf.Bucket, path string) (uint64, error) {
  closer := ctx.LogMark("[Object.LookupIdByBucketAndPath]")
  defer closer()

  tx := ctx.Txn()
  row := tx.QueryRow("SELECT id FROM object WHERE bucket_id = ? AND name = ?", bucketObj.Id, path )

  var id uint64
  err := row.Scan(&id)
  switch {
  case err == sql.ErrNoRows:
    ctx.Debugf("Could not find any object for %s/%s", bucketObj.Name, path)
    return 0, sql.ErrNoRows
  case err != nil:
    return 0, errors.New(fmt.Sprintf("Failed to execute query (LookupByBucketAndPath): %s", err))
  }

  ctx.Debugf("Loaded Object ID '%d' from %s/%s", id, bucketObj.Name, path)

  return id, nil
}

func LookupFromDB(
  ctx *context.RequestContext,
  id  uint64,
  o   *stf.Object,
) (error) {
  tx := ctx.Txn()
  row := tx.QueryRow("SELECT id, bucket_id, name, internal_name, size, status, created_at, updated_at  FROM object WHERE id = ?", id)

  err := row.Scan(
    &o.Id,
    &o.BucketId,
    &o.Name,
    &o.InternalName,
    &o.Size,
    &o.Status,
    &o.CreatedAt,
    &o.UpdatedAt,
  )

  if err != nil {
    ctx.Debugf("Failed to execute query (Lookup): %s", err)
    return err
  }

  return nil
}

func Lookup(ctx *context.RequestContext, id uint64) (*stf.Object, error) {
  closer := ctx.LogMark("[Object.Lookup]")
  defer closer()

  var o stf.Object
  cache := ctx.Cache()
  cacheKey := cache.CacheKey("object", strconv.FormatUint(id, 10))
  err := cache.Get(cacheKey, &o)
  if err == nil {
    ctx.Debugf("Cache HIT for object %d, returning object from cache", id)
    return &o, nil
  }

  err = LookupFromDB(ctx, id, &o)
  if err != nil {
    return nil, err
  }

  ctx.Debugf("Successfully loaded object %d from database", id)
  cache.Set(cacheKey, o, 3600)
  return &o, nil
}

func GetStoragesFor(
  ctx *context.RequestContext,
  objectObj *stf.Object,
) ([]stf.Storage, error) {
  closer := ctx.LogMark("[Object.GetStoragesFor]")
  defer closer()

  /* We cache
   *   "storages_for.$object_id => [ storage_id, storage_id, ... ]
   */
  cache       := ctx.Cache()
  cacheKey    := cache.CacheKey(
    "storages_for",
    strconv.FormatUint(objectObj.Id, 10),
  )
  var storageIds []uint32
  var list []stf.Storage

  err := cache.Get(cacheKey, &storageIds)

  if err == nil {
    // Cache HIT. we need to check for the validity of the storages
    list, err = storage.LookupMulti(ctx, storageIds)
    if err != nil {
      list = []stf.Storage {}
    } else {
      // Check each
    }
  }

  if len(list) == 0 {
    ctx.Debugf("Cache MISS for storages for object %d, loading from database", objectObj.Id)

    var storageIds []int64
    sql :=
      "SELECT s.id, s.uri, s.mode\n" +
      "   FROM object o JOIN entity e ON o.id = e.object_id\n" +
      "                 JOIN storage s ON s.id = e.storage_id\n" +
      "   WHERE\n" +
      "     o.id = ? AND\n" +
      "     o.status = 1 AND\n" +
      "     e.status = 1 AND\n" +
      "     s.mode IN (?, ?)"
    tx := ctx.Txn()
    rows, err := tx.Query(sql, objectObj.Id, stf.STORAGE_MODE_READ_ONLY, stf.STORAGE_MODE_READ_WRITE)
    if err != nil {
      return nil, err
    }

    for rows.Next() {
      var s stf.Storage
      err = rows.Scan(
        &s.Id,
        &s.Uri,
        &s.Mode,
      )
      storageIds = append(storageIds, int64(s.Id))
      list = append(list, s)
    }
ctx.Debugf("%+v", storageIds)
    cache.Set(cacheKey, storageIds, 600)
  }
  ctx.Debugf("Loaded %d storages", len(list))
  return list, nil
}

func EnqueueRepair(
  ctx *context.RequestContext,
  bucketObj *stf.Bucket,
  objectObj *stf.Object,
) {
  go func () {
    // This operation does not have to complete succesfully, so
    // so we use defer() here to eat any panic conditions that we
    // may encounter
    if err := recover(); err != nil {
      ctx.Debugf(
        "Error while sending object %d (%s/%s) to repair (ignored): %s",
        objectObj.Id,
        bucketObj.Name,
        objectObj.Name,
      )
    }
  }()

  ctx.Debugf(
    "Object %d (%s/%s) being sent to repair (harmless)",
    objectObj.Id,
    bucketObj.Name,
    objectObj.Name,
  )

  queue.Insert(
    ctx,
    "repair_object",
    strconv.FormatUint(objectObj.Id, 10),
  )

  // Putting this in memcached via Add() allows us from not sending
  // this object to repair repeatedly
  cache := ctx.Cache()
  cacheKey := cache.CacheKey(
    "repair_from_dispatcher",
    strconv.FormatUint(objectObj.Id, 10),
  )
  cache.Add(cacheKey, 1, 3600)
}

func GetAnyValidEntityUrl (
  ctx *context.RequestContext, 
  bucketObj *stf.Bucket,
  objectObj *stf.Object,
  doHealthCheck bool, // true if we want to run repair
  ifModifiedSince string,
) (string, error) {
  closer := ctx.LogMark("[Object.GetAnyValidEntityUrl]")
  defer closer()

  // XXX We have to do this before we check the entities, because in
  // real-life applications many of the requests come with an IMS header
  // which short-circuits from this method, and never allows us to
  // reach in this enqueuing block
  if doHealthCheck {
    defer func() {
      go EnqueueRepair(ctx, bucketObj, objectObj)
    }()
  }

  storages, err := GetStoragesFor(ctx, objectObj)
  if err != nil {
    return "", err
  }

  client := &http.Client {}
  for _, storage := range storages {
    ctx.Debugf("Attempting to make a request to %s (id = %d)", storage.Uri, storage.Id)
    url := fmt.Sprintf("%s/%s", storage.Uri, objectObj.InternalName)
    request, err := http.NewRequest("HEAD", url, nil)
    // if this is errornous, we're in deep shit
    if err != nil {
      return "", err
    }

    if ifModifiedSince != "" {
      request.Header.Set("If-Modified-Since", ifModifiedSince)
    }
    resp, err := client.Do(request)
    if err != nil {
      continue
    }

    switch resp.StatusCode {
    case 200:
      return url,nil 
    case 304:
      // This is wierd, but this is how we're gin
      return "", ErrContentNotModified
    default:
      // If we failed to fetch the object, send it to repair
      if ! doHealthCheck {
        doHealthCheck = true
        defer func() {
          go EnqueueRepair(ctx, bucketObj, objectObj)
        }()
      }
      // nothing more to do, try our next candidate
    }
  }

  // if we fell through here, we're done for
  return "", nil
}

func MarkForDelete (ctx *context.RequestContext, id uint64) error {
  tx := ctx.Txn()
  res, err := tx.Exec("REPLACE INTO deleted_object SELECT * FROM object WHERE id = ?", id)

  if err != nil {
    ctx.Debugf("Failed to execute query (REPLACE into deleted_object): %s", err)
    return err
  }

  if count, _ := res.RowsAffected(); count <= 0 {
    // Grr, we failed to insert to deleted_object table
    err = errors.New("Failed to insert object into deleted object queue")
    ctx.Debugf("%s", err)
    return err
  }

  res, err = tx.Exec("DELETE FROM object WHERE id = ?", id)
  if err != nil {
    ctx.Debugf("Failed to execute query (DELETE from object): %s", err)
    return err
  }

  if count, _ := res.RowsAffected(); count <= 0 {
    err = errors.New("Failed to delete object")
    ctx.Debugf("%s", err)
    return err
  }

  cache := ctx.Cache()
  cacheKey := cache.CacheKey("object", strconv.FormatUint(id, 10))
  err = cache.Delete(cacheKey)

  if err != nil && err.Error() != "memcache: cache miss" {
    ctx.Debugf("Failed to delete cache '%s': '%s'", cacheKey, err)
    return err
  }
  return nil
}

func Delete (ctx *context.RequestContext, id uint64) error {
  tx := ctx.Txn()
  _, err := tx.Exec("DELETE FROM object WHERE id = ?", id)
  if err != nil {
    return err
  }

  cache := ctx.Cache()
  cacheKey := cache.CacheKey("object", strconv.FormatUint(id, 10))
  err = cache.Delete(cacheKey)
  if err != nil {
    return err
  }

  return nil
}

func Create (
  ctx *context.RequestContext, 
  objectId uint64,
  bucketId uint64,
  objectName string,
  internalName string,
  size int64,
) error {
  closer := ctx.LogMark("[Object.Create]")
  defer closer()
  tx := ctx.Txn()
  _, err := tx.Exec("INSERT INTO object (id, bucket_id, name, internal_name, size, created_at) VALUES (?, ?, ?, ?, ?, UNIX_TIMESTAMP())", objectId, bucketId, objectName, internalName, size)

  if err != nil {
    ctx.Debugf("Failed to execute query: %s", err)
    return err
  }

  ctx.Debugf("Created object entry for '%d' (internal_name = '%s')", objectId, internalName)
  return nil
}

func AttemptCreate (
  ctx *context.RequestContext, 
  objectId uint64,
  bucketId uint64,
  objectName string,
  internalName string,
  size int64,
) (err error) {
  defer func () {
    if v := recover(); v != nil {
      err = v.(error) // this becomes the return value. woot
    }
  }()

  err = Create(
    ctx,
    objectId,
    bucketId,
    objectName,
    internalName,
    size,
  )

  return err
}

func createInternalName (suffix string) string {
  buf := make([]byte, 30)
  n, err := randbo.New().Read(buf)

  if err != nil {
    log.Fatalf("createInternalName failed: %s", err)
  }

  if n != len(buf) {
    log.Fatalf("createInternalName failed: (n = %d) != (len = %d)", n, len(buf))
  }

  hex := fmt.Sprintf("%x.%s", buf, suffix)

  return strings.Join(
    []string { hex[0:1], hex[1:2], hex[2:3], hex[3:4], hex, },
    "/",
  )
}

func Store (
  ctx *context.RequestContext,
  objectId uint64,
  bucketId uint64,
  objectName string,
  size int64,
  input *bytes.Reader,
  suffix string,
  isRepair bool,
  force bool,
) error {
  closer := ctx.LogMark("[Object.Store]")
  defer closer()

  done := false
  for i := 0; i < 10; i++ {
    internalName := createInternalName(suffix)
    err := AttemptCreate(
      ctx,
      objectId,
      bucketId,
      objectName,
      internalName,
      size,
    )
    if err == nil {
      done = true
      break
    } else {
      ctx.Debugf("Failed to create object in DB: %s", err)
    }
  }

  if ! done { // whoa, we fell through here w/o creating the object!
    err := errors.New("Failed to create object entry")
    ctx.Debugf("%s", err)
    return err
  }

  // After this point if something wicked happens and we bail out,
  // we don't want to keep this object laying around in a half-baked
  // state. So make sure to get rid of it
  done = false
  defer func() {
    if ! done {
      ctx.Debugf("Something went wrong, deleting object to make sure")
      Delete(ctx, objectId)
    }
  }()

  objectObj, err := Lookup(ctx, objectId)
  if err != nil {
    ctx.Debugf("Failed to lookup up object from DB: %s", err)
    return err
  }

  // Load all possible clusters ordered by a consistent hash
  clusters, err := cluster.LoadCandidatesFor(ctx, objectId)
  if err != nil {
    return err
  }

  if len(clusters) <= 0 {
    err := errors.New(fmt.Sprintf("No write candidate cluster found for object %d!", objectId))
    ctx.Debugf("%s", err)
    return err
  }

  for _, clusterObj := range clusters {
    err := cluster.Store(
      ctx,
      clusterObj.Id,
      objectObj,
      input,
      2,
      isRepair,
      force,
    )
    if err == nil { // Success!
      ctx.Debugf("Successfully stored objects in cluster %d", clusterObj.Id)
      cluster.RegisterForObject(
        ctx,
        clusterObj.Id,
        objectId,
      )
      // Set this to true so that the defered cleanup
      // doesn't get triggered
      done = true
      return nil
    }
    ctx.Debugf("Failed to store in cluster %d: %s", clusterObj.Id, err)
  }

  err = errors.New("Could not store in ANY clusters!")
  ctx.Debugf("%s", err)

  return err
}

