package stf

import (
  "bytes"
  "database/sql"
  "errors"
  "fmt"
  "io"
  "net/http"
  "strconv"
  "strings"
)

type Entity struct {
  ObjectId uint64
  StorageId uint64
  Status    int
}

type EntityApi struct {
  *BaseApi
}

func NewEntityApi (ctx ContextWithApi) *EntityApi {
  return &EntityApi { &BaseApi { ctx } }
}

func (self *EntityApi) Lookup(objectId uint64, storageId uint64) (*Entity, error) {
  ctx := self.Ctx()

  closer := LogMark("[Entity.Lookup]")
  defer closer()

  tx, err := ctx.Txn()

  row := tx.QueryRow(
    "SELECT status FROM entity WHERE object_id = ? AND storage_id = ?",
    objectId,
    storageId,
  )

  e := Entity { objectId, storageId, 0 }
  err = row.Scan(&e.Status)
  if err != nil {
    Debugf(
      "Failed to execute query (Entity.Lookup [%d, %d]): %s",
      objectId,
      storageId,
      err,
    )
    return nil, err
  }

  Debugf("Successfully loaded entity for object %d storage %d", objectId, storageId)
  return &e, nil
}

func (self *EntityApi) LookupFromRows(rows *sql.Rows) ([]*Entity, error) {
  closer := LogMark("[Entity.LookupFromRows]")
  defer closer()

  var list []*Entity
  for rows.Next() {
    e := &Entity{}
    err := rows.Scan(&e.ObjectId, &e.StorageId, &e.Status)
    if err != nil {
      return nil, err
    }
    list = append(list, e)
  }

  Debugf("Loaded %d entities", len(list))
  return list, nil
}

func (self *EntityApi) LookupForObjectNotInCluster (objectId uint64, clusterId uint64) ([]*Entity, error) {
  ctx := self.Ctx()

  closer := LogMark("[Entity.LookupForObjectNotInCluster]")
  defer closer()

  Debugf("Looking for entities for object %d", objectId)

  tx, err := ctx.Txn()

  rows, err := tx.Query(`
SELECT e.object_id, e.storage_id, e.status
  FROM entity e JOIN storage s ON e.storage_id = s.id
  WHERE object_id = ? AND s.cluster_id != ?
`,
    objectId,
    clusterId,
  )
  if err != nil {
    return nil, err
  }

  list, err := self.LookupFromRows(rows)
  if err != nil {
    return nil, err
  }

  Debugf(
    "Loaded %d entities for object %d (except cluster %d)",
    len(list),
    objectId,
    clusterId,
  )
  return list, nil
}

func (self *EntityApi) LookupForObject (objectId uint64) ([]*Entity, error) {
  ctx := self.Ctx()

  closer := LogMark("[Entity.LookupForObject]")
  defer closer()

  Debugf("Looking for entities for object %d", objectId)

  tx, err := ctx.Txn()

  rows, err := tx.Query(
    `SELECT e.object_id, e.storage_id, e.status FROM entity e WHERE object_id = ?`,
    objectId,
  )
  if err != nil {
    return nil, err
  }

  list, err := self.LookupFromRows(rows)
  if err != nil {
    return nil, err
  }

  Debugf("Loaded %d entities for object %d", len(list), objectId)
  return list, nil
}

func (self *EntityApi) Create (
  objectId uint64,
  storageId uint64,
) error {
  ctx := self.Ctx()

  closer := LogMark("[Entity.Create]")
  defer closer()

  tx, err := ctx.Txn()
  if err != nil {
    return err
  }

  _, err = tx.Exec("INSERT INTO entity (object_id, storage_id, status, created_at) VALUES (?, ?, 1, UNIX_TIMESTAMP())", objectId, storageId)
  if err != nil {
    Debugf("Failed to execute query: %s", err)
    return err
  }

  Debugf("Created entity entry for '%d', '%d'", objectId, storageId)
  return nil
}

func (self *EntityApi) FetchContent(o *Object, s *Storage, isRepair bool) (io.ReadCloser, error) {
  ctx := self.Ctx()

  closer := LogMark("[Entity.FetchContent]")
  defer closer()

  storageApi := ctx.StorageApi()
  if ! storageApi.IsReadable(s, isRepair) {
    return nil, errors.New(
      fmt.Sprintf(
        "Storage %d is not readable",
        s.Id,
      ),
    )
  }

  return self.FetchContentNocheck(o, s, isRepair)
}

func (self *EntityApi) FetchContentNocheck (
  o *Object,
  s *Storage,
  isRepair bool,
) (io.ReadCloser, error) {
  closer := LogMark("[Entity.FetchContentNocheck]")
  defer closer()

  Debugf(
    "Fetching content from storage %d",
    s.Id,
  )

  client := &http.Client{}

  uri := strings.Join([]string{ s.Uri, o.InternalName }, "/")
  Debugf(
    "Sending GET %s (object = %d, storage = %d)",
    uri,
    o.Id,
    s.Id,
  )

  // XXX Original perl version used to optimize the content fetch
  // here by writing the content into the file system in chunks.
  // Does go need/have such a mechanism?
  resp, err := client.Get(uri)
  if err != nil {
    return nil, err
  }

  var okStr string
  if resp.StatusCode == 200 {
    okStr = "OK"
  } else {
    okStr = "FAIL"
  }
  Debugf(
    "        GET %s was %s (%s)",
    uri,
    okStr,
    resp.StatusCode,
  )

  if resp.ContentLength != o.Size {
    Debugf(
      "Fetched content size for object %d does not match registered size?! (got %d, expected %d)",
      o.Id,
      resp.ContentLength,
      o.Size,
    )
    return nil, errors.New("Content size mismatch")
  }

  Debugf(
    "Success fetching %s (object = %d, storage = %d)",
    uri,
    o.Id,
    s.Id,
  )

  return resp.Body, nil
}

func (self *EntityApi) FetchContentFromStorageIds(o *Object, list []uint64, isRepair bool) (io.ReadCloser, error) {
  ctx := self.Ctx()

  closer := LogMark("[Entity.FetchContentFromStorageIds]")
  defer closer()

  storageApi := ctx.StorageApi()
  storages, err := storageApi.LookupMulti(list)
  if err != nil {
    return nil, err
  }

  for _, s := range storages {
    content, err := self.FetchContentNocheck(o, s, isRepair)
    if err == nil {
      return content, nil
    }
  }

  return nil, errors.New("Failed to fetch any content")
}

func (self *EntityApi) FetchContentFromAll (o *Object, isRepair bool) (io.ReadCloser, error) {
  ctx := self.Ctx()

  closer := LogMark("[Entity.FetchContentFromAll]")
  defer closer()

  sql := "SELECT s.id FROM storage s ORDER BY rand()"
  tx, err := ctx.Txn()
  if err != nil {
    return nil, err
  }

  rows, err := tx.Query(sql)

  var list []uint64
  for rows.Next() {
    var sid uint64
    err = rows.Scan(&sid)
    if err != nil {
      return nil, err
    }

    list = append(list, sid)
  }

  return self.FetchContentFromStorageIds(o, list, isRepair)
}

func (self *EntityApi) FetchContentFromAny (o *Object, isRepair bool) (io.ReadCloser, error) {
  ctx := self.Ctx()

  closer := LogMark("[Entity.FetchContentFromAny]")
  defer closer()

  sql := `
SELECT s.id
  FROM storage s JOIN entity e ON s.id = e.storage_id
  WHERE s.mode IN (?, ?) AND e.object_id = ?
  ORDER BY rand()
`

  tx, err := ctx.Txn()
  if err != nil {
    return nil, err
  }

  rows, err := tx.Query(sql, STORAGE_MODE_READ_ONLY, STORAGE_MODE_READ_WRITE, o.Id)
  if err != nil {
    return nil, err
  }

  var list []uint64
  for rows.Next() {
    var sid uint64
    err = rows.Scan(&sid)
    if err != nil {
      return nil, err
    }

    list = append(list, sid)
  }

  return self.FetchContentFromStorageIds(o, list, isRepair)
}

func (self *EntityApi) Store(
  storageObj  *Storage,
  objectObj   *Object,
  input       *bytes.Reader,
) error {
  closer := LogMark("[Entity.Store]")
  defer closer()

  uri := strings.Join([]string { storageObj.Uri, objectObj.InternalName }, "/")
  cl  := input.Len()

  Debugf("Going to store %d bytes in %s", cl, uri)

  req, err := http.NewRequest("PUT", uri, input)
  if err != nil {
    Debugf("Failed to create request: %s", err)
    return err
  }

  // XXX Need to check if this vanilla http client is ok
  client := &http.Client {}
  resp, err := client.Do(req)
  if err != nil {
    Debugf("Failed to send PUT request to %s (storage = %d): %s", uri, storageObj.Id, err)
    return err
  }

  if resp.StatusCode != 201 {
    err = errors.New(
      fmt.Sprintf(
        "Expected response 201 for PUT request, but did not get it: %s",
        resp.Status,
      ),
    )
    Debugf("Failed to store PUT request to %s (storage = %d): %s", uri, storageObj.Id, err)
    return err
  }

  Debugf("Successfully stored object in %s", uri)

  err = self.Create(
    objectObj.Id,
    storageObj.Id,
  )

  if err != nil {
    return err
  }

  return nil
}

// Proceed with caution!!!! THIS WILL DELETE THE ENTIRE ENTITY SET!
func (self *EntityApi) DeleteOrphansForObjectId(objectId uint64) error {
  ctx := self.Ctx()

  closer := LogMark("[Entity.DeletedOrphasForObjectId]")
  defer closer()

  tx, err := ctx.Txn()
  if err != nil {
    return err
  }

  _, err = tx.Exec("DELETE FROM entity WHERE object_id = ?", objectId)
  return err
}

func (self *EntityApi) RemoveForDeletedObjectId(objectId uint64) error {
  closer := LogMark("[EntityRemoveForDeletedObjectId]")
  defer closer()

  // Find existing entities 
  entities, err := self.LookupForObject(objectId)
  if err != nil {
    return err
  }
  for _, e := range entities {
    err = self.RemoveDeleted(e, true)
    if err != nil {
      return err
    }
  }

  return nil
}

func (self *EntityApi) CheckHealth(o *Object, s *Storage, isRepair bool) error {
  ctx := self.Ctx()

  closer := LogMark("[Entity.CheckHealth]")
  defer closer()

  Debugf("Checking entity health on object %d storage %d", o.Id, s.Id)

  _, err := self.Lookup(o.Id, s.Id)
  if err != nil {
    Debugf(
      "Entity on storage %d for object %d is not recorded.",
      s.Id,
      o.Id,
    )
    return errors.New(
      fmt.Sprintf(
        "Could not find entity in database: %s",
        err,
      ),
    )
  }

  // An entity in TEMPORARILY_DOWN node needs to be treated as alive
  if s.Mode == STORAGE_MODE_TEMPORARILY_DOWN {
    Debugf(
      "Storage %d is temporarily down. Assuming this is intact.",
      s.Id,
    )
    return nil
  }

  // If the mode is not in a readable state, then we've purposely 
  // taken it out of the system, and needs to be repaired. Also, 
  // if this were the case, we DO NOT issue an DELETE on the backend, 
  // as it most likely will not properly respond.

  storageApi := ctx.StorageApi()
  if ! storageApi.IsReadable(s, isRepair) {
    Debugf(
      "Storage %d is not reable. Adding to invalid list.",
      s.Id,
    )
    return errors.New("Storage is down")
  }

  url := strings.Join([]string{ s.Uri, o.InternalName }, "/")
  Debugf(
    "Going to check %s (object_id = %d, storage_id = %d)",
    url,
    o.Id,
    s.Id,
  )

  client := &http.Client {}
  res, err := client.Get(url)

  var okStr string
  var st    int
  if err != nil {
    okStr = "FAIL"
    st    = 500
  } else if res.StatusCode != 200 {
    okStr = "FAIL"
    st    = res.StatusCode
  } else {
    okStr = "OK"
    st    = res.StatusCode
  }

  Debugf(
    "GET %s was %s (%d)",
    url,
    okStr,
    st,
  )

  if err != nil {
    return errors.New("An error occurred while trying to fetch entity")
  }

  if res.StatusCode != 200 {
    return errors.New(
      fmt.Sprintf(
        "Failed to fetch entity: %s",
        res.Status,
      ),
    )
  }

  if res.ContentLength != o.Size {
    Debugf(
      "Object %d sizes do not match (got %d, expected %d)",
      o.Id,
      res.ContentLength,
      o.Size,
    )
    return errors.New("Object size mismatch")
  }

  return nil
}

func (self *EntityApi) SetStatus(e *Entity, st int) error {
  ctx := self.Ctx()

  closer := LogMark("[Entity.SetStatus]")
  defer closer()

  tx, err := ctx.Txn()
  if err != nil {
    return err
  }

  _, err = tx.Exec(
    "UPDATE entity SET status = ? WHERE object_id = ? AND storage_id = ?",
    st,
    e.ObjectId,
    e.StorageId,
  )

  if err != nil {
    Debugf(
      "Failed to set status of entity (object %d storage %d) to %d)",
      e.ObjectId,
      e.StorageId,
      st,
    )
    return err
  }

  Debugf(
    "Successfully set status of entity (object %d storage %d) to %d)",
    e.ObjectId,
    e.StorageId,
    st,
  )
  return nil
}

func (self *EntityApi) Delete (objectId uint64, storageId uint64) error {
  ctx := self.Ctx()

  closer := LogMark("[Entity.Delete]")
  defer closer()

  tx, err := ctx.Txn()
  if err != nil {
    return err
  }

  _, err = tx.Exec(
    "DELETE FROM entity WHERE object_id = ? AND storage_id = ?",
    objectId,
    storageId,
  )

  if err != nil {
    Debugf(
      "Failed to delete logical entity (object %d, storage %d): %s",
      objectId,
      storageId,
      err,
    )
    return err
  }

  Debugf(
    "Successfully deleted logical entity (object %d, storage %d)",
    objectId,
    storageId,
  )

  return nil
}

func (self *EntityApi) Remove (e *Entity, isRepair bool) error {
  return self.removeInternal(
    e,
    isRepair,
    false, // useDeletedObject: "no"
  )
}

func (self *EntityApi) RemoveDeleted (e *Entity, isRepair bool) error {
  return self.removeInternal(
    e,
    isRepair,
    true, // useDeletedObject: "yes"
  )
}

func (self *EntityApi) removeInternal(e *Entity, isRepair bool, useDeletedObject bool) error {
  ctx := self.Ctx()

  closer := LogMark("[Entity.Remove]")
  defer closer()

  self.Delete(e.ObjectId, e.StorageId)

  cache := ctx.Cache()
  cacheKey := cache.CacheKey(
    "storage",
    strconv.FormatUint(e.StorageId, 10),
    "http_accessible",
  )
  var httpAccesibleFlg int64
  err := cache.Get(cacheKey, &httpAccesibleFlg)
  if err == nil && httpAccesibleFlg == -1 {
    Debugf(
      "Storage %d was previously unaccessible, skipping physical delete",
      e.StorageId,
    )
    return errors.New("Storage is inaccessible (negative cache)")
  }

  storageApi := ctx.StorageApi()
  s, err := storageApi.Lookup(e.StorageId)
  if err != nil {
    return err
  }

  if ! storageApi.IsWritable(s, isRepair) {
    Debugf("Storage %d is not writable (isRepair = %s)", s.Id, isRepair)
    return errors.New("Storage is not writable")
  }

  var internalName string
  if useDeletedObject {
    o, err := ctx.DeletedObjectApi().Lookup(e.ObjectId)
    if err != nil {
      return err
    }
    internalName = o.InternalName
  } else {
    o, err := ctx.ObjectApi().Lookup(e.ObjectId)
    if err != nil {
      return err
    }
    internalName = o.InternalName
  }

  uri := strings.Join([]string { s.Uri, internalName }, "/")
  req, err := http.NewRequest("DELETE", uri, nil)
  client := &http.Client {}
  res, err := client.Do(req)
  if err != nil {
    // If you got here, the 'error' is usually error in DNS resolution
    // or connection refused and such. Remember this incident via a
    // negative cache, so that we don't keep on
    cache.Set(cacheKey, -1, 300)
    return err
  }

  switch {
  case res.StatusCode == 404:
    Debugf("%s was not found while deleting (ignored)", uri)
  case res.StatusCode >= 200 && res.StatusCode < 300:
    Debugf("Successfully deleted %s", uri)
  default:
    Debugf("An error occurred while deleting %s: %s", uri, res.Status)
  }

  return nil
}
