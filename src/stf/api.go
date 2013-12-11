package stf

import (
  "time"
)

type StfObject struct {
  Id        uint64
  CreatedAt int
  UpdatedAt time.Time
}

type Api interface {
  Ctx() Context

  Create(args ...interface{}) (error)

  // Lookup consults cache
  Lookup(id uint64) (*StfObject, error)

  // LookupFromDB always retrieves from DB
  LookupFromDB(id uint64) (*StfObject, error)

  // Deletes from DB and cache
  Delete(id uint64) (error)
}

type BaseApi struct {
  ctx ContextWithApi
}

func (self *BaseApi) Ctx() ContextWithApi { return self.ctx }
