/*

=head1 NAME

Bucket - STF Bucket

=head1 SYNOPSIS

    b := bucket.Load(1234)

*/

package bucket

import (
  "errors"
  "stf"
  "stf/context"
  "stf/queue"
  "strconv"
  "time"
)

func LookupIdByName(ctx *context.RequestContext, name string) (uint64, error) {

  closer := ctx.LogMark("[Bucket.LookupIdByName]")
  defer closer()

  ctx.Debugf("Looking for bucket '%s'", name)

  var id uint64

  tx := ctx.Txn()
  row := tx.QueryRow("SELECT id FROM bucket WHERE name = ?", name)

  err := row.Scan(&id)
  if err != nil {
    return 0, err
  }

  ctx.Debugf("Found id '%d' for bucket '%s'", id, name)
  return id, nil
}

// Used to load this from cache
func NewFromMap (st map[interface{}]interface{}) *stf.Bucket {
  rawUpdatedAt := st["UpdatedAt"].([]interface{})
  b := stf.Bucket {
    Id: st["Id"].(uint64),
    Name: st["Name"].(string),
    UpdatedAt: time.Unix(rawUpdatedAt[0].(int64), rawUpdatedAt[1].(int64)),
  }

  return &b
}

func LookupFromDB(
  ctx *context.RequestContext,
  id uint64,
  b   *stf.Bucket,
) (error) {
  tx  := ctx.Txn()
  row := tx.QueryRow("SELECT id, name FROM bucket WHERE id = ?", id)
  err := row.Scan(&b.Id, &b.Name)
  if err != nil {
    ctx.Debugf("Failed to scan query: %s", err)
    return err
  }
  return nil
}

func Lookup(ctx *context.RequestContext, id uint64) (*stf.Bucket, error) {
  closer := ctx.LogMark("[Bucket.Lookup]")
  defer closer()

  var b stf.Bucket
  cache := ctx.Cache()
  cacheKey := cache.CacheKey("bucket", strconv.FormatUint(id, 10))
  err := cache.Get(cacheKey, &b)

  if err == nil {
    ctx.Debugf("Cache HIT. Loaded from Memcached")
    return &b, nil
  }

  ctx.Debugf("Cache MISS. Loading from database")

  err = LookupFromDB(ctx, id, &b)
  if err != nil {
    return nil, err
  }
  ctx.Debugf("Successfully looked up bucket '%d' from DB", b.Id)
  cache.Set(cacheKey, b, 600)

  return &b, nil;
}

func Create(ctx *context.RequestContext, id uint64, name string) error {
  closer := ctx.LogMark("[Bucket.Create]")
  defer closer()

  tx := ctx.Txn()
  _, err := tx.Exec(
    "INSERT INTO bucket (id, name, created_at, updated_at) VALUES (?, ?, UNIX_TIMESTAMP(), NOW())",
    id,
    name,
  )

  if err != nil {
    return err
  }

  ctx.Debugf("Created bucket '%s' (id = %d)", name, id)

  return nil
}

func MarkForDelete(ctx *context.RequestContext, id uint64) error {
  tx := ctx.Txn()

  res, err := tx.Exec("REPLACE INTO deleted_bucket SELECT * FROM bucket WHERE id = ?", id)

  if err != nil {
    ctx.Debugf("Failed to execute query (REPLACE into deleted_bucket): %s", err)
    return err
  }

  if count, _ := res.RowsAffected(); count <= 0 {
    // Grr, we failed to insert to deleted_bucket table
    err = errors.New("Failed to insert bucket into deleted bucket queue")
    ctx.Debugf("%s", err)
    return err
  }

  res, err = tx.Exec("DELETE FROM bucket WHERE id = ?", id)
  if err != nil {
    ctx.Debugf("Failed to execute query (DELETE from bucket): %s", err)
    return err
  }

  if count, _ := res.RowsAffected(); count <= 0 {
    err = errors.New("Failed to delete bucket")
    ctx.Debugf("%s", err)
    return err
  }

  cache := ctx.Cache()
  cache.Delete( cache.CacheKey("bucket", strconv.FormatUint(id, 10)) )

  return nil
}

func DeleteObjects(ctx *context.RequestContext, id uint64) error {
  tx := ctx.Txn()

  rows, err := tx.Query("SELECT id FROM object WHERE bucket_id = ?", id)
  if err != nil {
    ctx.Debugf("Failed to execute query: %s", err)
    return err
  }

  var objectId uint64
  for rows.Next() {
    err = rows.Scan(&objectId)
    if err != nil {
      ctx.Debugf("Failed to scan from query: %s", err)
      return err
    }

    err = queue.Insert(ctx, "delete_object", strconv.FormatUint(objectId, 10))
    if err != nil {
      ctx.Debugf("Failed to insert object ID in delete_object queue: %s", err)
    }
  }

  _, err = tx.Exec("DELETE FROM deleted_bucket WHERE id = ?", id)
  if err != nil {
    ctx.Debugf("Failed to delete bucket from deleted_bucket: %s", err)
  }

  return nil
}

func Delete(ctx *context.RequestContext, id uint64, recursive bool) error {
  if recursive {
    err := DeleteObjects(ctx, id)
    if err != nil {
      return err
    }
  }
  return nil
}
