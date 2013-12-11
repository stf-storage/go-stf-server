package stf

import (
  "fmt"
  "strconv"
  "strings"
)

type Storage struct {
  ClusterId     uint64
  Uri           string
  Mode          int
  StfObject
}

type StorageApi struct {
  *BaseApi
}

func NewStorageApi (ctx ContextWithApi) *StorageApi {
  return &StorageApi { &BaseApi { ctx } }
}

func (self *StorageApi) LookupFromDB(id uint32) (*Storage, error) {
  ctx := self.Ctx()

  closer := ctx.LogMark("[Storage.LookupFromDB]")
  defer closer()

  tx, err := ctx.Txn()
  if err != nil {
    return nil, err
  }
  row := tx.QueryRow("SELECT id, cluster_id, uri, mode, created_at, updated_at FROM storage WHERE id = ?", id)

  var s Storage
  err = row.Scan(
    &s.Id,
    &s.ClusterId,
    &s.Uri,
    &s.Mode,
    &s.CreatedAt,
    &s.UpdatedAt,
  )

  if err != nil {
    ctx.Debugf("Failed to execute query (StorageLookup): %s", err)
    return &s, err
  }

  ctx.Debugf("Successfully loaded storage %d from database", id)

  return &s, nil
}

func (self *StorageApi) Lookup(id uint32) (*Storage, error) {
  ctx := self.Ctx()

  closer := ctx.LogMark("[Storage.StorageLookup]")
  defer closer()

  var s Storage
  cache := ctx.Cache()
  cacheKey := cache.CacheKey("storage", strconv.FormatUint(uint64(id), 10))
  err := cache.Get(cacheKey, &s)
  if err == nil {
    ctx.Debugf("Cache HIT for storage %d, returning storage from cache", id)
    return &s, nil
  }

  ctx.Debugf("Cache MISS for '%s', fetching from database", cacheKey)

  sptr, err := self.LookupFromDB(id)
  if err != nil {
    return nil, err
  }

  cache.Set(cacheKey, *sptr, 3600)
  return sptr, nil
}

func (self *StorageApi) LookupMulti(ids []uint32) ([]Storage, error) {
  ctx := self.Ctx()

  closer := ctx.LogMark("[Storage.LookupMulti]")
  defer closer()

  cache := ctx.Cache()

  var keys []string
  for _, id := range ids {
    key  := cache.CacheKey("storage", strconv.FormatUint(uint64(id), 10))
    keys = append(keys, key)
  }

  var cached map[string]interface {}
  cached, err := cache.GetMulti(keys, func() interface {} { return &Storage {} })
  if err != nil {
    ctx.Debugf("GetMulti failed: %s", err)
    return nil, err
  }

  var ret []Storage
  for _, id := range ids {
    key  := cache.CacheKey("storage", strconv.FormatUint(uint64(id), 10))
    st, ok := cached[key].(Storage)

    if ! ok {
      s, err := self.Lookup(id)
      if err != nil {
        return nil, err
      }
      st = *s
    }
    ret = append(ret, st)
  }
  return ret, nil
}

func (self *StorageApi) LoadWritable(clusterId uint64, isRepair bool) ([]*Storage, error) {
  ctx := self.Ctx()
  closer := ctx.LogMark("[Storage.LoadWritableStorages]")
  defer closer()

  placeholders := []string {}
  binds := []interface {} { clusterId, }
  var modes []int
  if isRepair {
    modes = WRITABLE_MODES_ON_REPAIR
  } else {
    modes = WRITABLE_MODES
  }

  ctx.Debugf("Repair flag is '%v', using %+v for modes", isRepair, modes)

  for _, v := range modes {
    binds = append(binds, v)
    placeholders = append(placeholders, "?")
  }

  sql := fmt.Sprintf(
    "SELECT id FROM storage WHERE cluster_id = ? AND mode IN (%s)",
    strings.Join(placeholders, ", "),
  )

  tx, err := ctx.Txn()
  if err != nil {
    return nil, err
  }

  rows, err := tx.Query(sql, binds...)
  if err != nil {
    return nil, err
  }

  var ids []uint32
  var list []*Storage
  var id uint32
  for rows.Next() {
    err = rows.Scan(&id)
    if err != nil {
      return nil, err
    }

    ids = append(ids, id)
  }

  for _, id = range ids {
    s, err := self.Lookup(id)
    if err != nil {
      return nil, err
    }
    // WTF s can be nil? well, let's just not append this guy
    if s != nil {
      list = append(list, s)
    }
  }

  ctx.Debugf("Loaded %d storages", len(list))

  return list, nil
}
