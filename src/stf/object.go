package stf

import (
  "bytes"
  "database/sql"
  "errors"
  "fmt"
  "io/ioutil"
  "log"
  "net/http"
  "strconv"
  "strings"
  randbo "github.com/dustin/randbo"
)

type Object struct {
  BucketId      uint64
  Name          string
  InternalName  string
  Size          int64
  Status        int
  StfObject
}

type ObjectApi struct {
  *BaseApi
}

var ErrContentNotModified error = errors.New("Request Content Not Modified")

func NewObjectApi (ctx ContextWithApi) *ObjectApi {
  return &ObjectApi { &BaseApi { ctx } }
}

func (self *ObjectApi) LookupIdByBucketAndPath(bucketObj *Bucket, path string) (uint64, error) {
  ctx := self.Ctx()

  closer := ctx.LogMark("[Object.LookupIdByBucketAndPath]")
  defer closer()

  tx, err := ctx.Txn()
  if err != nil {
    return 0, err
  }

  row := tx.QueryRow("SELECT id FROM object WHERE bucket_id = ? AND name = ?", bucketObj.Id, path )

  var id uint64
  err = row.Scan(&id)
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

func (self *ObjectApi) LookupFromDB(id  uint64) (*Object, error) {
  ctx := self.Ctx()

  tx, err := ctx.Txn()
  if err != nil {
    return nil, err
  }

  row := tx.QueryRow("SELECT id, bucket_id, name, internal_name, size, status, created_at, updated_at  FROM object WHERE id = ?", id)

  var o Object
  err = row.Scan(
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
    return nil, err
  }

  return &o, nil
}

func (self *ObjectApi) Lookup(id uint64) (*Object, error) {
  ctx := self.Ctx()

  closer := ctx.LogMark("[Object.Lookup]")
  defer closer()

  var o Object
  cache := ctx.Cache()
  cacheKey := cache.CacheKey("object", strconv.FormatUint(id, 10))
  err := cache.Get(cacheKey, &o)
  if err == nil {
    ctx.Debugf("Cache HIT for object %d, returning object from cache", id)
    return &o, nil
  }

  optr, err := self.LookupFromDB(id)
  if err != nil {
    return nil, err
  }

  ctx.Debugf("Successfully loaded object %d from database", id)
  cache.Set(cacheKey, *optr, 3600)
  return optr, nil
}

func (self *ObjectApi) GetStoragesFor(objectObj *Object) ([]*Storage, error) {
  ctx := self.Ctx()
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
  var storageIds []uint64
  var list []*Storage

  err := cache.Get(cacheKey, &storageIds)

  if err == nil {
    // Cache HIT. we need to check for the validity of the storages
    list, err = ctx.StorageApi().LookupMulti(storageIds)
    if err != nil {
      list = []*Storage {}
    } else {
      // Check each
    }
  }

  if len(list) == 0 {
    ctx.Debugf("Cache MISS for storages for object %d, loading from database", objectObj.Id)

    var storageIds []int64
    sql := `
SELECT s.id, s.uri, s.mode
  FROM object o JOIN entity e ON o.id = e.object_id
                JOIN storage s ON s.id = e.storage_id
  WHERE o.id     = ? AND
        o.status = 1 AND
        e.status = 1 AND
        s.mode   IN (?, ?)
`

    tx, err := ctx.Txn()
    if err != nil {
      return nil, err
    }

    rows, err := tx.Query(sql, objectObj.Id, STORAGE_MODE_READ_ONLY, STORAGE_MODE_READ_WRITE)
    if err != nil {
      return nil, err
    }

    for rows.Next() {
      s := Storage {}
      err = rows.Scan(
        &s.Id,
        &s.Uri,
        &s.Mode,
      )
      storageIds = append(storageIds, int64(s.Id))
      list = append(list, &s)
    }
    if len(storageIds) > 0 {
      cache.Set(cacheKey, storageIds, 600)
    }
  }
  ctx.Debugf("Loaded %d storages", len(list))
  return list, nil
}

func (self *ObjectApi) EnqueueRepair(
  bucketObj *Bucket,
  objectObj *Object,
) {
  ctx := self.Ctx()

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

  queueApi := ctx.QueueApi()
  queueApi.Enqueue("repair_object", strconv.FormatUint(objectObj.Id, 10))

  // Putting this in memcached via Add() allows us from not sending
  // this object to repair repeatedly
  cache := ctx.Cache()
  cacheKey := cache.CacheKey(
    "repair_from_dispatcher",
    strconv.FormatUint(objectObj.Id, 10),
  )
  cache.Add(cacheKey, 1, 3600)
}

func (self *ObjectApi) GetAnyValidEntityUrl (
  bucketObj *Bucket,
  objectObj *Object,
  doHealthCheck bool, // true if we want to run repair
  ifModifiedSince string,
) (string, error) {
  ctx := self.Ctx()
  closer := ctx.LogMark("[Object.GetAnyValidEntityUrl]")
  defer closer()

  // XXX We have to do this before we check the entities, because in
  // real-life applications many of the requests come with an IMS header
  // which short-circuits from this method, and never allows us to
  // reach in this enqueuing block
  if doHealthCheck {
    defer func() {
      go self.EnqueueRepair(bucketObj, objectObj)
    }()
  }

  storages, err := self.GetStoragesFor(objectObj)
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
      ctx.Debugf("Failed to send request to %s: %s", url, err)
      continue
    }

    switch resp.StatusCode {
    case 200:
      ctx.Debugf("Request successs, returning URL '%s'", url)
      return url,nil
    case 304:
      // This is wierd, but this is how we're going to handle it
      ctx.Debugf("Request target is not modified, returning 304")
      return "", ErrContentNotModified
    default:
      ctx.Debugf("Request for %s failed: %s", url, resp.Status)
      // If we failed to fetch the object, send it to repair
      if ! doHealthCheck {
        doHealthCheck = true
        defer func() {
          go self.EnqueueRepair(bucketObj, objectObj)
        }()
      }
      // nothing more to do, try our next candidate
    }
  }

  // if we fell through here, we're done for
  err = errors.New("Could not find a valid entity in any of the storages")
  ctx.Debugf("%s", err)
  return "", err
}

func (self *ObjectApi) MarkForDelete (id uint64) error {
  ctx := self.Ctx()
  tx, err := ctx.Txn()
  if err != nil {
    return err
  }

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

func (self *ObjectApi) Delete (id uint64) error {
  ctx := self.Ctx()
  tx, err := ctx.Txn()
  if err != nil {
    return err
  }

  _, err = tx.Exec("DELETE FROM object WHERE id = ?", id)
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

func (self *ObjectApi) Create (
  objectId uint64,
  bucketId uint64,
  objectName string,
  internalName string,
  size int64,
) error {
  ctx := self.Ctx()
  closer := ctx.LogMark("[Object.Create]")
  defer closer()
  tx, err := ctx.Txn()
  if err != nil {
    return err
  }

  _, err = tx.Exec("INSERT INTO object (id, bucket_id, name, internal_name, size, created_at) VALUES (?, ?, ?, ?, ?, UNIX_TIMESTAMP())", objectId, bucketId, objectName, internalName, size)

  if err != nil {
    ctx.Debugf("Failed to execute query: %s", err)
    return err
  }

  ctx.Debugf("Created object entry for '%d' (internal_name = '%s')", objectId, internalName)
  return nil
}

func (self *ObjectApi) AttemptCreate (
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

  err = self.Create(
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

func (self *ObjectApi) Store (
  objectId uint64,
  bucketId uint64,
  objectName string,
  size int64,
  input *bytes.Reader,
  suffix string,
  isRepair bool,
  force bool,
) error {
  ctx := self.Ctx()

  closer := ctx.LogMark("[Object.Store]")
  defer closer()

  done := false
  for i := 0; i < 10; i++ {
    internalName := createInternalName(suffix)
    err := self.AttemptCreate(
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
      self.Delete(objectId)
    }
  }()

  objectObj, err := self.Lookup(objectId)
  if err != nil {
    ctx.Debugf("Failed to lookup up object from DB: %s", err)
    return err
  }

  // Load all possible clusters ordered by a consistent hash
  clusterApi := ctx.StorageClusterApi()
  clusters, err := clusterApi.LoadCandidatesFor(objectId)
  if err != nil {
    return err
  }

  if len(clusters) <= 0 {
    err := errors.New(fmt.Sprintf("No write candidate cluster found for object %d!", objectId))
    ctx.Debugf("%s", err)
    return err
  }

  for _, clusterObj := range clusters {
    err := clusterApi.Store(
      clusterObj.Id,
      objectObj,
      input,
      2,
      isRepair,
      force,
    )
    if err == nil { // Success!
      ctx.Debugf("Successfully stored objects in cluster %d", clusterObj.Id)
      clusterApi.RegisterForObject(
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

var ErrNothingToRepair = errors.New("Nothing to repair")
func (self *ObjectApi) Repair (objectId uint64) error {
  ctx := self.Ctx()

  closer := ctx.LogMark("[Object.Repair]")
  defer closer()

  ctx.Debugf(
    "Repairing object %d",
    objectId,
  )

  entityApi := ctx.EntityApi()
  o, err := self.Lookup(objectId)
  if err != nil {
    ctx.Debugf("No matching object %d", objectId)

    entities, err := entityApi.LookupForObject(objectId)
    if err != nil {
      return ErrNothingToRepair
    }

    if ctx.DebugLog() != nil {
      ctx.Debugf("Removing orphaned entities in storages:")
      for _, e := range entities {
        ctx.Debugf(" + %d", e.StorageId)
      }
    }
    entityApi.DeleteAllForObject(objectId)
    return ErrNothingToRepair
  }

  ctx.Debugf(
    "Fetching master content for object %d from any of the known entities",
    o.Id,
  )
  masterContent, err := entityApi.FetchContentFromAny(o, true)
  if err != nil {
    // One more shot. See if we can recover the content from ANY
    // of the available storages
    masterContent, err = entityApi.FetchContentFromAll(o, true)
    if err != nil {
      return errors.New(
        fmt.Sprintf(
          "PANIC: No content for %d could be fetched!! Cannot proceed with repair.",
          objectId,
        ),
      )
    }
  }

  ctx.Debugf("Successfully fetched master content")

  clusterApi := ctx.StorageClusterApi()
  clusters, err := clusterApi.LoadCandidatesFor(objectId)
  if err != nil || len(clusters) < 1 {
    return errors.New(
      fmt.Sprintf(
        "Could not find any storage candidate for object %d",
        objectId,
      ),
    )
  }

  // Keep this empty until successful completion of the next 
  // `if err == nil {...} else {...} block. It serves as a marker that
  // the object is stored in this cluster
  var designatedCluster *StorageCluster

  // The object SHOULD be stored in the first instance
  ctx.Debugf(
    "Checking entity health on cluster %d",
    clusters[0].Id,
  )
  err = clusterApi.CheckEntityHealth(clusters[0], o, true)
  needsRepair := err != nil

  if ! needsRepair {
    // No need to repair. Just make sure object -> cluster mapping
    // is intact
    ctx.Debugf(
      "Object %d is correctly stored in cluster %d. Object does not need repair",
      objectId,
      clusters[0].Id,
    )

    designatedCluster = clusters[0]
    currentCluster, _ := clusterApi.LookupForObject(objectId)
    if currentCluster == nil || designatedCluster.Id != currentCluster.Id {
      // Ignore errors. No harm done
      clusterApi.RegisterForObject(designatedCluster.Id, objectId)
    }
  } else {
    // Need to repair
    ctx.Debugf("Object %d needs repair", objectId)

    // If it got here, either the object was not properly in designatedCluster
    // (i.e., some/all of the storages in the cluster did not have this
    // object stored) or it was in a different cluster
    contentBuf, err := ioutil.ReadAll(masterContent)
    if err != nil {
      return errors.New(
        fmt.Sprintf(
          "Failed to read from content handle: %s",
          err,
        ),
      )
    }
    contentReader := bytes.NewReader(contentBuf)
    for _, cluster := range clusters {
      ctx.Debugf(
        "Attempting to store object %d on cluster %d",
        o.Id,
        cluster.Id,
      )
      err = clusterApi.Store(
        cluster.Id,
        o,
        contentReader,
        0, // minimumTosTore
        true, // isRepair
        false, // force
      )
      if err == nil {
        designatedCluster = cluster
        ctx.Debugf(
          "Successfully stored object %d on cluster %d",
          o.Id,
          cluster.Id,
        )
        break
      }
      ctx.Debugf(
        "Failed to store object %d on cluster %d: %s",
        o.Id,
        cluster.Id,
        err,
      )
    }

    if designatedCluster == nil {
      return errors.New(
        fmt.Sprintf(
          "PANIC: Failed to repair object %d to any cluster!",
          objectId,
        ),
      )
    }
  }

  // Object is now properly stored in designatedCluster. Find which storages
  // map to this cluster, and remove any other entities, if available.
  // This may happen if we added new clusters and rebalancing ocurred
  entities, err := entityApi.LookupForObjectNotInCluster(objectId, designatedCluster.Id)

  // Cache needs to be invalidated regardless, but we should be careful
  // about the timing
  cache     := ctx.Cache()
  cacheKey  := cache.CacheKey("storages_for", strconv.FormatUint(objectId, 10))

  cacheInvalidator := func() {
    ctx.Debugf("Invalidating cache %s", cacheKey)
    cache.Delete(cacheKey)
  }

  if needsRepair {
    defer cacheInvalidator()
  }

  // Note: this err is from entityApi.LookupForObject
  if err != nil {
    ctx.Debugf("Failed to fetch entities for object %d: %s", objectId, err)
  } else if entities != nil && len(entities) > 0 {
    ctx.Debugf("Extra entities found: dropping status flag, then proceeding to remove %d entities", len(entities))
    for _, e := range entities {
      entityApi.SetStatus(e, 0)
    }

    // Make sure to invalidate the cache here, because we don't want
    // the dispatcher to pick the entities with status = 0
    cacheInvalidator()

    for _, e := range entities {
      entityApi.Remove(e, true)
    }
  }

  ctx.Debugf("Done repair for object %d", objectId)
  return nil
}
