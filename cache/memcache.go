package cache

import (
  "crypto/md5"
  "errors"
  "fmt"
  "io"
  "log"
  "strings"
  "github.com/bradfitz/gomemcache/memcache"
  "github.com/vmihailenco/msgpack"
  "github.com/stf-storage/go-stf-server"
)

type MemdClient struct {
  client *memcache.Client
}

type Int64Value struct {
  Value int64
}

type StringValue struct {
  Value string
}

func NewMemdClient(args ...string) *MemdClient {
  memd := memcache.New(args...)
  return &MemdClient { client: memd }
}

func (self *MemdClient) CacheKey (args ...string) string {
  // Prepend with our custom namespace
  parts := append([]string { "go-stf", fmt.Sprintf("version(%s)", stf.VERSION) }, args...)
  key := strings.Join(parts, ".")
  // Encode keys that are too long
  if len(key) > 250 {
    h := md5.New()
    io.WriteString(h, key)
    key = fmt.Sprintf("%x", h.Sum(nil))
  }
  return key
}

func (self *MemdClient) Add (key string, value interface {}, expires int32) error {
  b, err := msgpack.Marshal(value)
  if err != nil {
    log.Fatalf("Failed to encode value: %s", err)
  }
  item := &memcache.Item { Key: key,  Value: b, Flags: 4, Expiration: expires }
  return self.client.Add(item)
}

func (self *MemdClient) Set (key string, value interface {}, expires int32) error {
  b, err := msgpack.Marshal(value)
  if err != nil {
    log.Fatalf("Failed to encode value: %s", err)
  }
  item := &memcache.Item { Key: key,  Value: b, Flags: 4, Expiration: expires }
  return self.client.Set(item)
}

func (self *MemdClient) GetMulti(
  keys []string,
  makeContainer func() interface {},
) (map[string]interface {}, error) {
  items, err := self.client.GetMulti(keys)
  if err != nil {
    return nil, err
  }

  ret := make(map[string]interface {})
  for k, item := range items {
    v := makeContainer()
    err := msgpack.Unmarshal(item.Value, v)
    if err != nil {
      continue
    }
    ret[k] = v
  }

  return ret, nil
}

func (self *MemdClient) Get(key string, v interface {}) (error) {
  item, err := self.client.Get(key)

  if err != nil {
    return err
  }

  if item.Flags & 4 != 0 {
    err := msgpack.Unmarshal(item.Value, &v)
    if err != nil {
      err = errors.New(fmt.Sprintf("Failed to decode value: %s", err))
      return err
    }
    return nil
  }

  v = item.Value
  return nil
}

func (self *MemdClient) Delete (key string) error {
  return self.client.Delete(key)
}

