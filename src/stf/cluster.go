package stf

import (
  "bytes"
  "crypto/md5"
  "errors"
  "fmt"
  "io"
  "io/ioutil"
  "sort"
  "strconv"
)

type StorageCluster struct {
  StfObject
  Name      string
  Mode      int
  sortHint  uint32
}

type StorageClusterApi struct {
  *BaseApi
}

func NewStorageClusterApi (ctx ContextWithApi) *StorageClusterApi {
  return &StorageClusterApi { &BaseApi { ctx } }
}

// These are defined to allow sorting via the sort package
type ClusterCandidates []StorageCluster

func (self ClusterCandidates) Prepare(objectId uint64) {
  idStr := strconv.FormatUint(objectId, 10)
  for _, x := range self {
    key := strconv.FormatUint(uint64(x.Id), 10) + idStr
    x.sortHint = MurmurHash([]byte(key))
  }
}
func (self ClusterCandidates) Len() int { return len(self) }
func (self ClusterCandidates) Swap(i, j int) { self[i], self[j] = self[j], self[i] }
func (self ClusterCandidates) Less(i, j int) bool {
  return self[i].sortHint < self[j].sortHint
}

func (self *StorageClusterApi) LoadWritable () (ClusterCandidates, error) {
  ctx := self.Ctx()

  closer := ctx.LogMark("[Cluster.LoadWritable]")
  defer closer()

  tx, err := ctx.Txn()
  if err != nil {
    return nil, err
  }

  rows, err := tx.Query("SELECT id, name, mode FROM storage_cluster WHERE mode = ?", STORAGE_CLUSTER_MODE_READ_WRITE)

  if err != nil {
    ctx.Debugf("Failed to execute query: %s", err)
    return nil, err
  }

  var list ClusterCandidates
  for rows.Next() {
    var s StorageCluster
    err = rows.Scan(&s.Id, &s.Name, &s.Mode)
    if err != nil {
      return nil, err
    }

    list = append(list, s)
  }

  ctx.Debugf("Loaded %d clusters", len(list))

  return list, nil
}

func (self *StorageClusterApi) LookupFromDB(id uint64) (*StorageCluster, error) {
  ctx := self.Ctx()

  closer := ctx.LogMark("[StorageCluster.LookupFromDB]")
  defer closer()

  tx, err := ctx.Txn()
  if err != nil {
    return nil, err
  }

  row := tx.QueryRow("SELECT name, mode FROM storage_cluster WHERE id = ?", id)

  c := StorageCluster { StfObject { Id: id }, "", 0, 0 }
  err = row.Scan(&c.Name, &c.Mode)
  if err != nil {
    ctx.Debugf("Failed to execute query (Lookup): %s", err)
    return nil, err
  }

  return &c, nil
}

func (self *StorageClusterApi) Lookup(id uint64) (*StorageCluster, error) {
  ctx := self.Ctx()

  closer := ctx.LogMark("[StorageCluster.Lookup]")
  defer closer()

  var c StorageCluster
  cache := ctx.Cache()
  cacheKey := cache.CacheKey("storage_cluster", strconv.FormatUint(id, 10))
  err := cache.Get(cacheKey, &c)

  if err == nil {
    ctx.Debugf("Cache HIT for cluster %d, returning cluster from cache", id)
    return &c, nil
  }

  cptr, err := self.LookupFromDB(id)
  if err != nil {
    return nil, err
  }

  ctx.Debugf("Successfully loaded cluster %d from database", id)
  cache.Set(cacheKey, *cptr, 3600)
  return cptr, nil
}

func (self *StorageClusterApi) LookupForObject(objectId uint64) (*StorageCluster, error) {
  ctx := self.Ctx()

  closer := ctx.LogMark("[StorageCluster.LookupForObject]")
  defer closer()

  tx, err := ctx.Txn()
  if err != nil {
    return nil, err
  }

  var cid uint64
  row := tx.QueryRow("SELECT cluster_id FROM object_cluster_map WHERE object_id = ?", objectId)
  err = row.Scan(&cid)
  if err != nil {
    return nil, err
  }

  return self.Lookup(cid)
}

func (self *StorageClusterApi) LoadCandidatesFor(objectId uint64) (ClusterCandidates, error) {
  ctx := self.Ctx()
  closer := ctx.LogMark("[Cluster.LoadCandidatesFor]")
  defer closer()

  list, err := self.LoadWritable()
  if err != nil {
    return nil, err
  }
  list.Prepare(objectId)
  sort.Sort(list)

  return list, nil
}

func calcMD5 (input interface { Read([]byte) (int, error) } ) []byte {
  h := md5.New()
  var buf []byte
  for {
    n, err := input.Read(buf)
    if (n > 0) {
      io.WriteString(h, string(buf))
    }
    if n == 0 || err == io.EOF {
      break
    }
  }

  return h.Sum(nil)
}

func (self *StorageClusterApi) Store(
  clusterId uint64,
  o *Object,
  input *bytes.Reader,
  minimumToStore int,
  isRepair bool,
  force bool,
) error {
  ctx := self.Ctx()

  closer := ctx.LogMark("[Cluster.Store]")
  defer closer()

  storageApi := ctx.StorageApi()
  storages, err := storageApi.LoadWritable(clusterId, isRepair)
  if err != nil {
    ctx.Debugf("Failed to load storage candidates for writing: %s", err)
    return err
  }

  if minimumToStore < 1 {
    // Give it a default value
    minimumToStore = 3
  }

  if len(storages) < minimumToStore {
    err = errors.New(
      fmt.Sprintf(
        "Only loaded %d storages (wanted %d) for cluster %d",
        len(storages),
        minimumToStore,
        clusterId,
      ),
    )
    ctx.Debugf("%s", err)
    return err
  }

  var expected []byte
  if ! force {
    // Micro-optimize
    expected = calcMD5(input)
  }

  entityApi := ctx.EntityApi()

  stored := 0
  for _, s := range storages {
    ctx.Debugf("Attempting to store to storage %s (id = %d)", s.Uri, s.Id)
    // Without the force flag, we fetch the object before storing to
    // avoid redundant writes. force should only be used when you KNOW
    // that this is a new entity
    var fetchedContent []byte
    var fetchedMD5 []byte
    fetchOK := false
    if ! force {
      if fetched, err := entityApi.FetchContent(o, s, isRepair); err == nil {
        if fetchedContent, err = ioutil.ReadAll(fetched); err == nil {
          fetchedMD5 = calcMD5(bytes.NewReader(fetchedContent))
          fetchOK = true
        }
      }
    }

    if fetchOK {
      // Find the MD5 checksum of the fetchedContent, and make sure that
      // this indeed matches what we want to store
      if bytes.Equal(fetchedMD5, expected) {
        // It's a match!
        ctx.Debugf("Entity on storage %d exist, and md5 matches. Assume this is OK")
        break
      }
    }

    if _, err = input.Seek(0, 0); err != nil {
      err = errors.New(fmt.Sprintf("failed to seek: %s", err))
      return err
    }

    err = entityApi.Store(s, o, input)
    if err == nil {
      stored++
      if minimumToStore > 0 && stored >= minimumToStore {
        break
      }
    }
  }

  storedOK := minimumToStore == 0 || stored >= minimumToStore

  if ! storedOK {
    return errors.New(
      fmt.Sprintf(
        "Only stored %d entities while we wanted %d entities",
        stored,
        minimumToStore,
      ),
    )
  }

  ctx.Debugf(
    "Stored %d entities for object %d in cluster %d",
    stored,
    o.Id,
    clusterId,
  )
  return nil
}

func (self *StorageClusterApi) RegisterForObject(clusterId uint64, objectId uint64) error {
  return nil
}

var ErrClusterNotWritable = errors.New("Cluster is not writable")
var ErrNoStorageAvailable = errors.New("No storages available")
func (self *StorageClusterApi) CheckEntityHealth(
  cluster   *StorageCluster,
  o         *Object,
  isRepair  bool,
) error {
  ctx := self.Ctx()

  closer := ctx.LogMark("[StorageCluster.CheckEntityHealth]")
  defer closer()

  ctx.Debugf(
    "Checking entity health for object %d on cluster %d",
    o.Id,
    cluster.Id,
  )

  // Short circuit. If the cluster mode is not rw or ro, then
  // we have a problem.
  if cluster.Mode != STORAGE_CLUSTER_MODE_READ_WRITE && cluster.Mode != STORAGE_CLUSTER_MODE_READ_ONLY {
    ctx.Debugf(
      "Cluster %d is not read-write or read-only, need to move object %d out of this cluster",
      cluster.Id,
      o.Id,
    )
    return ErrClusterNotWritable
  }

  storageApi := ctx.StorageApi()
  storages, err := storageApi.LoadInCluster(cluster.Id)
  if err != nil {
    return ErrNoStorageAvailable
  }

  entityApi := ctx.EntityApi()
  for _, s := range storages {
    err = entityApi.CheckHealth(o, s, isRepair)
    if err != nil {
      ctx.Debugf("Health check for entity on object %d storage %d failed", o.Id, s.Id)
      return errors.New(
        fmt.Sprintf(
          "Entity for object %d on storage %d is unavailable: %s",
          o.Id,
          s.Id,
          err,
        ),
      )
    }
  }
  return nil
} 
