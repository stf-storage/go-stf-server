package stf

import (
  "bytes"
  "errors"
  "fmt"
  "net/http"
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

func (self *EntityApi) Create (
  objectId uint64,
  storageId uint64,
) error {
  ctx := self.Ctx()

  closer := ctx.LogMark("[Entity.Create]")
  defer closer()

  tx, err := ctx.Txn()
  if err != nil {
    return err
  }

  _, err = tx.Exec("INSERT INTO entity (object_id, storage_id, status, created_at) VALUES (?, ?, 1, UNIX_TIMESTAMP())", objectId, storageId)
  if err != nil {
    ctx.Debugf("Failed to execute query: %s", err)
    return err
  }

  ctx.Debugf("Created entity entry for '%d', '%d'", objectId, storageId)
  return nil
}

func (self *EntityApi) FetchContent(
  object *Object,
  storageId uint64,
  isRepair bool,
) ([]byte, error) {
  ctx := self.Ctx()
  closer := ctx.LogMark("[Entity.FetchContent]")
  defer closer()
  return nil, nil
}

func (self *EntityApi) Store(
  storageObj  *Storage,
  objectObj   *Object,
  input       *bytes.Reader,
) error {
  ctx := self.Ctx()

  closer := ctx.LogMark("[Entity.Store]")
  defer closer()

  uri := strings.Join([]string { storageObj.Uri, objectObj.InternalName }, "/")
  cl  := input.Len()

  ctx.Debugf("Going to store %d bytes in %s", cl, uri)

  req, err := http.NewRequest("PUT", uri, input)
  if err != nil {
    ctx.Debugf("Failed to create request: %s", err)
    return err
  }

  // XXX Need to check if this vanilla http client is ok
  client := &http.Client {}
  resp, err := client.Do(req)
  if err != nil {
    ctx.Debugf("Failed to send PUT request to %s (storage = %d): %s", uri, storageObj.Id, err)
    return err
  }

  if resp.StatusCode != 201 {
    err = errors.New(
      fmt.Sprintf(
        "Expected response 201 for PUT request, but did not get it: %s",
        resp.Status,
      ),
    )
    ctx.Debugf("Failed to store PUT request to %s (storage = %d): %s", uri, storageObj.Id, err)
    return err
  }

  ctx.Debugf("Successfully stored object in %s", uri)

  err = self.Create(
    objectObj.Id,
    storageObj.Id,
  )

  if err != nil {
    return err
  }

  return nil
}

