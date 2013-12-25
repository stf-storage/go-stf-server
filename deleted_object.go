package stf

type DeletedObject Object

type DeletedObjectApi struct {
  *BaseApi
}

func NewDeletedObjectApi (ctx ContextWithApi) *DeletedObjectApi {
  return &DeletedObjectApi { &BaseApi { ctx } }
}

// No caching
func (self *DeletedObjectApi) Lookup(id uint64) (*DeletedObject, error) {
  ctx := self.Ctx()

  tx, err := ctx.Txn()
  if err != nil {
    return nil, err
  }

  row := tx.QueryRow("SELECT id, bucket_id, name, internal_name, size, status, created_at, updated_at  FROM deleted_object WHERE id = ?", id)

  var o DeletedObject
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

