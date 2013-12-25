package stf

import (
  "fmt"
  "sync"
  "time"
)

const (
  EPOCH_OFFSET  = 946684800
  HOST_ID_BITS  = 16
  SERIAL_BITS   = 12
  SERIAL_SHIFT  = 16
  TIME_BITS     = 36
  TIME_SHIFT    = 28
)

type UUIDGen struct {
  seed      uint64
  mutex     sync.Mutex
  serialId  int64
  timeId    int64
  timeout   int64
}

func (self *UUIDGen) CreateId () uint64 {
  /* Only one thread can enter this critical section, but it also must be
     guarded carefully so that we don't find this thread being blocked
     for an indefinite amount of time
  */

  timeout := time.AfterFunc(time.Duration(self.timeout) * time.Second, func () {
    panic(
      fmt.Sprintf("CreateId(): Failed to acquire lock in time (timeout = %d seconds)", self.timeout),
    )
  })

  self.mutex.Lock()
  /* Make absolutely sure that we unlock by registering defer */
  defer self.mutex.Unlock()

  /* We got here? cancel the timer */
  timeout.Stop()

  timeId    := time.Now().Unix()
  serialId  := self.serialId
  if self.timeId == timeId {
    serialId++
  } else {
    serialId = 1
  }

  if serialId >= (1 << SERIAL_BITS) - 1 {
    // Overflow:/ we recieved more than SERIAL_BITS
    panic("Serial bits overflowed")
  }

  self.serialId = serialId
  self.timeId   = timeId

  timeBits    := (timeId - EPOCH_OFFSET) << TIME_SHIFT
  serialBits  := serialId << SERIAL_SHIFT

  id := uint64(timeBits) | uint64(serialBits) | self.seed

  return id
}