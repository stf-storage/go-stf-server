package stf

import (
  "time"
)

type StfObject struct {
  Id        uint64
  CreatedAt int
  UpdatedAt time.Time
}

type BaseApi struct {
  ctx ContextWithApi
}

func (self *BaseApi) Ctx() ContextWithApi { return self.ctx }
