package data

import (
  "time"
)

type StfObject struct {
  Id        uint64
  CreatedAt int
  UpdatedAt time.Time
}

type Object struct {
  StfObject
  BucketId      uint64
  Name          string
  InternalName  string
  Size          int64
  Status        int
}

type Bucket struct {
  StfObject
  Name          string
}

type StorageCluster struct {
  StfObject
  Name      string
  Mode      int
  SortHint  uint32
}

type Storage struct {
  StfObject
  ClusterId     uint64
  Uri           string
  Mode          int
}

type Entity struct {
  ObjectId uint64
  StorageId uint64
  Status    int
}

type DeletedObject Object


