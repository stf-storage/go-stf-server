package stf

import (
  "bytes"
  "crypto/md5"
  "errors"
  "fmt"
  "io"
  "sort"
  "strconv"
)

type StorageCluster struct {
  Name string
  Mode int
  sortHint uint32
  StfObject
}

type StorageClusterApi struct {
  *BaseApi
}

func NewStorageClusterApi (ctx *RequestContext) *StorageClusterApi {
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
  objectObj *Object,
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

  var expected []byte
  if ! force {
    // Micro-optimize
    expected = calcMD5(input)
  }

  entityApi := ctx.EntityApi()

  stored := 0
  for _, storageObj := range storages {
    ctx.Debugf("Attempting to store to storage %s (id = %d)", storageObj.Uri, storageObj.Id)
    // Without the force flag, we fetch the object before storing to
    // avoid redundant writes. force should only be used when you KNOW
    // that this is a new entity
    var fetchedContent []byte
    var fetchedMD5 []byte
    fetchOK := false
    if ! force {
      fetchedContent, err = entityApi.FetchContent(
        objectObj,
        storageObj.Id,
        isRepair,
      )

      if err != nil {
        fetchOK = true
      }

      fetchedMD5 = calcMD5(bytes.NewReader(fetchedContent))
    }

    if fetchOK {
      // Find the MD5 checksum of the fetchedContent, and make sure that
      // this indeed matches what we want to store
      if ! bytes.Equal(fetchedMD5, expected) {
        panic("md5 does not match")
      }
    }

    if _, err = input.Seek(0, 0); err != nil {
      err = errors.New(fmt.Sprintf("failed to seek: %s", err))
      return err
    }

    err = entityApi.Store(
      storageObj,
      objectObj,
      input,
    )

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
  return nil
}

func (self *StorageClusterApi) RegisterForObject(clusterId uint64, objectId uint64) error {
  return nil
}
