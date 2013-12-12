package stf

import (
  "crypto/sha1"
  "fmt"
  "io"
  "strconv"
  "time"
)

const VERSION = "0.0.1"

func GenerateRandomId(hint string, length int) string {
  h := sha1.New()
  io.WriteString(h, hint)
  io.WriteString(h, strconv.FormatInt(time.Now().UTC().UnixNano(), 10))
  return (fmt.Sprintf("%x", h.Sum(nil)))[0:length]
}
