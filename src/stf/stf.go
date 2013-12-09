package stf

import (
  "bytes"
  "encoding/binary"
  "log"
  "time"
)

type GlobalConfig struct {
  Debug     bool
}

type DispatcherConfig struct {
  ServerId  uint64
  Listen    string
}

type DatabaseConfig struct {
  Dbtype    string
  Username  string
  Password  string
  Protocol  string
  Hostname  string
  Port      uint32
  Dbname    string
}

type MemcachedConfig struct {
  Servers   []string
}

type Config struct {
  Dispatcher  DispatcherConfig
  Global      GlobalConfig
  MainDB      DatabaseConfig
  Memcached   MemcachedConfig
  QueueDB     []DatabaseConfig
}


type Bucket struct {
  Id uint64
  Name string
  CreatedAt     int64
  UpdatedAt     time.Time
}

type Object struct {
  Id            uint64
  BucketId      uint64
  Name          string
  InternalName  string
  Size          int
  Status        int
  CreatedAt     int
  UpdatedAt     time.Time
}

type Storage struct {
  Id            uint32
  ClusterId     uint32
  Uri           string
  Mode          int
  CreatedAt     int
  UpdatedAt     time.Time
}

// Imeplementation of murmurHash (ver 1) in go
func MurmurHash (data []byte) uint32 {
  const m uint32 = 0x5bd1e995
  const r uint8  = 16
  var length uint32 = uint32(len(data))
  var h      uint32 = length * m

  nblocks := int(length / 4)
  buf := bytes.NewBuffer(data)
  for i := 0; i < nblocks; i++ {
    var x uint32
    err := binary.Read(buf, binary.LittleEndian, &x)
    if err != nil {
       log.Fatal("Failed to read from buffer")
    }
    h += x
    h *= m
    h ^= h >> r
  }

  tailIndex := nblocks * 4
  switch length & 3 {
  case 3:
    h += uint32(data[tailIndex + 2]) << 16
    fallthrough
  case 2:
    h += uint32(data[tailIndex + 1]) << 8
    fallthrough
  case 1:
    h += uint32(data[tailIndex])
    h *= m
    h ^= h >> r
  }

  h *= m
  h ^= h >> 10
  h *= m
  h ^= h >> 17

  return h
}


