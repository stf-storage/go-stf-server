
package worker

import (
  "errors"
  "fmt"
  "github.com/stf-storage/go-stf-server"
  "log"
  "net/http"
  "strconv"
  "strings"
  "time"
)

type StorageHealthWorker struct {
  *BaseWorker
}

func NewStorageHealthWorker() *StorageHealthWorker {
  f := NewIntervalFetcher(900 * time.Second)
  w := &StorageHealthWorker { NewBaseWorker(f) }
  w.WorkCb = w.Work

  return w
}

func (self *StorageHealthWorker) Work(arg *stf.WorkerArg) (err error) {
  ctx := self.ctx
  closer, err := ctx.TxnBegin()
  if err != nil {
    return
  }
  defer closer()

  sql := `SELECT id, uri FROM storage WHERE mode IN (?, ?)`
  db, err := ctx.MainDB()
  if err != nil {
    return
  }

  rows, err := db.Query(sql, stf.STORAGE_MODE_READ_ONLY, stf.STORAGE_MODE_READ_WRITE)
  if err != nil {
    return
  }

  var storages []*stf.Storage
  for rows.Next() {
    var s stf.Storage

    err = rows.Scan(&s.Id, &s.Uri)
    if err != nil {
      return
    }
    storages = append(storages, &s)
  }

  for _, s := range storages {
    if err = self.StorageIsAvailable(s); err != nil {
      log.Printf(`
CRITICAL! FAILED TO PUT/HEAD/GET/DELETE to storage 
  error       : %s
  storage id  : %d
  storage uri : %s
GOING TO BRING DOWN THIS STORAGE!
`,
        err,
        s.Id,
        s.Uri,
      )
      if err = self.MarkStorageDown(ctx, s); err != nil {
        log.Printf("Failed to mark storage as down: %s", err)
        return
      }
    }
  }
  ctx.TxnCommit()

  return
}

func (self *StorageHealthWorker) StorageIsAvailable(s *stf.Storage) (err error) {
  uri     := strings.Join([]string{ s.Uri, "health.txt" }, "/")
  content := stf.GenerateRandomId(uri, 40)
  client  := &http.Client {}

  // Delete the object first, just in case
  // Note: No error checks here
  req, _ := http.NewRequest("DELETE", uri, nil)
  res, _ := client.Do(req)

  // Now do a successibe PUT/HEAD/GET/DELETE

  req, err = http.NewRequest("PUT", uri, strings.NewReader(content))
  if err != nil {
    return
  }
  res, err = client.Do(req)
  if err != nil {
    return
  }
  if res.StatusCode != 201 {
    return errors.New(fmt.Sprintf("Failed to PUT %s: %s", uri, res.Status))
  }

  req, err = http.NewRequest("HEAD", uri, nil)
  if err != nil {
    return
  }
  res, err = client.Do(req)
  if err != nil {
    return
  }
  if res.StatusCode != 200 {
    return errors.New(fmt.Sprintf("Failed to HEAD %s: %s", uri, res.Status))
  }

  req, err = http.NewRequest("GET", uri, nil)
  if err != nil {
    return
  }
  res, err = client.Do(req)
  if err != nil {
    return
  }
  if res.StatusCode != 200 {
    return errors.New(fmt.Sprintf("Failed to GET %s: %s", uri, res.Status))
  }

  req, err = http.NewRequest("DELETE", uri, nil)
  if err != nil {
    return
  }
  res, err = client.Do(req)
  if err != nil {
    return
  }
  if res.StatusCode != 200 {
    return errors.New(fmt.Sprintf("Failed to DELETE %s: %s", uri, res.Status))
  }

  return
}

func (self *StorageHealthWorker) MarkStorageDown(ctx *stf.Context, s *stf.Storage) (err error) {
  db, err := ctx.MainDB()
  if err != nil {
    return
  }
  sql := `UPDATE storage SET mode = ?, updated_at = NOW() WHERE id = ?`
  _, err = db.Exec(sql, stf.STORAGE_MODE_TEMPORARILY_DOWN, s.Id)
  if err != nil {
    return
  }

  // Kill the cache
  cache := ctx.Cache()
  cacheKey := cache.CacheKey("storage", strconv.FormatUint(uint64(s.Id), 10))
  cache.Delete(cacheKey)

  return
}