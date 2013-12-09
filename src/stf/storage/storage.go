package storage

import (
  "fmt"
  "stf"
  "stf/context"
  "strconv"
  "strings"
  "time"
)

func NewFromMap(st map[interface{}]interface {}) *stf.Storage {
  rawUpdatedAt := st["UpdatedAt"].([]interface{})
  s := stf.Storage{
    Id: uint32(st["Id"].(int64)),
    Uri: st["Uri"].(string),
    Mode: int(st["Mode"].(int64)),
    CreatedAt: int(st["CreatedAt"].(int64)),
    UpdatedAt: time.Unix(rawUpdatedAt[0].(int64), rawUpdatedAt[1].(int64)),
  }
  return &s
}

func LookupFromDB(
  ctx *context.RequestContext,
  id uint32,
  s   *stf.Storage,
) (error) {
  tx := ctx.Txn()
  row := tx.QueryRow("SELECT id, cluster_id, uri, mode, created_at, updated_at FROM storage WHERE id = ?", id)

  err := row.Scan(
    &s.Id,
    &s.ClusterId,
    &s.Uri,
    &s.Mode,
    &s.CreatedAt,
    &s.UpdatedAt,
  )

  if err != nil {
    ctx.Debugf("Failed to execute query (Lookup): %s", err)
    return err
  }

  ctx.Debugf("Successfully loaded storage %d from database", id)

  return nil
}

func Lookup(ctx *context.RequestContext, id uint32) (*stf.Storage, error) {
  closer := ctx.LogMark("[Storage.Lookup]")
  defer closer()

  var s stf.Storage
  cache := ctx.Cache()
  cacheKey := cache.CacheKey("storage", strconv.FormatUint(uint64(id), 10))
  err := cache.Get(cacheKey, &s)
  if err == nil {
    ctx.Debugf("Cache HIT for storage %d, returning storage from cache", id)
    return &s, nil
  }

  ctx.Debugf("Cache MISS for '%s', fetching from database", cacheKey)

  err = LookupFromDB(ctx, id, &s)
  if err != nil {
    return nil, err
  }

  cache.Set(cacheKey, s, 3600)
  return &s, nil
}

func LookupMulti(
  ctx *context.RequestContext,
  ids []uint32,
) ([]stf.Storage, error) {

  cache := ctx.Cache()

  var keys []string
  for _, id := range ids {
    key  := cache.CacheKey("storage", strconv.FormatUint(uint64(id), 10))
    keys = append(keys, key)
  }

  var cached map[string]interface {}
  cached, err := cache.GetMulti(keys, func() interface {} { return &stf.Storage {} })
  if err != nil {
    return nil, err
  }

  var ret []stf.Storage
  for _, id := range ids {
    key  := cache.CacheKey("storage", strconv.FormatUint(uint64(id), 10))
    st, ok := cached[key].(stf.Storage)

    if ! ok {
      err = LookupFromDB(ctx, id, &st)
      if err != nil {
        return nil, err
      }
    }
    ret = append(ret, st)
  }
  return ret, nil
}

func LoadWritable(ctx *context.RequestContext, clusterId uint32, isRepair bool) ([]*stf.Storage, error) {
  closer := ctx.LogMark("[Storage.LoadWritableStorages]")
  defer closer()

  placeholders := []string {}
  binds := []interface {} { clusterId, }
  var modes []int
  if isRepair {
    modes = stf.WRITABLE_MODES_ON_REPAIR
  } else {
    modes = stf.WRITABLE_MODES
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

  tx := ctx.Txn()
  rows, err := tx.Query(sql, binds...)
  if err != nil {
    return nil, err
  }

  var ids []uint32
  var list []*stf.Storage
  var id uint32
  for rows.Next() {
    err = rows.Scan(&id)
    if err != nil {
      return nil, err
    }

    ids = append(ids, id)
  }

  for _, id = range ids {
    s, err := Lookup(ctx, id)
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
