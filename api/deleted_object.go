package api

import (
  "github.com/stf-storage/go-stf-server"
  "github.com/stf-storage/go-stf-server/data"
)

type DeletedObject struct {
  *BaseApi
}

func NewDeletedObject (ctx ContextWithApi) *DeletedObject {
  return &DeletedObject { &BaseApi { ctx } }
}

// No caching
func (self *DeletedObject) Lookup(id uint64) (*data.DeletedObject, error) {
  ctx := self.Ctx()

  tx, err := ctx.Txn()
  if err != nil {
    return nil, err
  }

  row := tx.QueryRow("SELECT id, bucket_id, name, internal_name, size, status, created_at, updated_at  FROM deleted_object WHERE id = ?", id)

  var o data.DeletedObject
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
    stf.Debugf("Failed to execute query (Lookup): %s", err)
    return nil, err
  }

  return &o, nil
}

