package stf

import (
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
  mutex     chan int
  serialId  int64
  timeId    int64
  timeout   int64
}

func NewIdGenerator (seed uint64) *UUIDGen {
  return &UUIDGen {
    seed: seed,
    mutex: make(chan int, 1),
    serialId: 1,
    timeId: time.Now().Unix(),
  }
}

func (self *UUIDGen) CreateId () uint64 {
  /* Only one thread can enter this critical section, but it also must be
     guarded carefully so that we don't find this thread being blocked
     for an indefinite amount of time
  */
  mutex := self.mutex
  mutex <-1
  defer func() { <-mutex }()

  timeId    := time.Now().Unix()
  serialId  := self.serialId
  if self.timeId == 0 {
    self.timeId = timeId
  }

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