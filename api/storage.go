package api

import (
	"fmt"
	"github.com/stf-storage/go-stf-server"
	"github.com/stf-storage/go-stf-server/data"
	"strconv"
	"strings"
)

type Storage struct {
	*BaseApi
}

func NewStorage(ctx ContextWithApi) *Storage {
	return &Storage{&BaseApi{ctx}}
}

func (self *Storage) LookupFromDB(id uint64) (*data.Storage, error) {
	ctx := self.Ctx()

	closer := ctx.LogMark("[Storage.LookupFromDB]")
	defer closer()

	tx, err := ctx.Txn()
	if err != nil {
		return nil, err
	}
	row := tx.QueryRow("SELECT id, cluster_id, uri, mode, created_at, updated_at FROM storage WHERE id = ?", id)

	var s data.Storage
	err = row.Scan(
		&s.Id,
		&s.ClusterId,
		&s.Uri,
		&s.Mode,
		&s.CreatedAt,
		&s.UpdatedAt,
	)

	if err != nil {
		ctx.Debugf("Failed to execute query (StorageLookup): %s", err)
		return &s, err
	}

	ctx.Debugf("Successfully loaded storage %d from database", id)

	return &s, nil
}

func (self *Storage) Lookup(id uint64) (*data.Storage, error) {
	ctx := self.Ctx()

	closer := ctx.LogMark("[Storage.StorageLookup]")
	defer closer()

	var s data.Storage
	cache := ctx.Cache()
	cacheKey := cache.CacheKey("storage", strconv.FormatUint(uint64(id), 10))
	err := cache.Get(cacheKey, &s)
	if err == nil {
		ctx.Debugf("Cache HIT for storage %d, returning storage from cache", id)
		return &s, nil
	}

	ctx.Debugf("Cache MISS for '%s', fetching from database", cacheKey)

	sptr, err := self.LookupFromDB(id)
	if err != nil {
		return nil, err
	}

	cache.Set(cacheKey, *sptr, 3600)
	return sptr, nil
}

func (self *Storage) LookupFromSql(sql string, binds []interface{}) ([]*data.Storage, error) {
	ctx := self.Ctx()

	tx, err := ctx.Txn()
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(sql, binds...)
	if err != nil {
		return nil, err
	}

	var ids []uint64
	for rows.Next() {
		var sid uint64
		err = rows.Scan(&sid)
		if err != nil {
			return nil, err
		}
		ids = append(ids, sid)
	}

	return self.LookupMulti(ids)
}

func (self *Storage) LookupMulti(ids []uint64) ([]*data.Storage, error) {
	ctx := self.Ctx()

	closer := ctx.LogMark("[Storage.LookupMulti]")
	defer closer()

	cache := ctx.Cache()

	var keys []string
	for _, id := range ids {
		key := cache.CacheKey("storage", strconv.FormatUint(id, 10))
		keys = append(keys, key)
	}

	var cached map[string]interface{}
	cached, err := cache.GetMulti(keys, func() interface{} { return &Storage{} })
	if err != nil {
		ctx.Debugf("GetMulti failed: %s", err)
		return nil, err
	}

	var ret []*data.Storage
	misses := 0
	for _, id := range ids {
		key := cache.CacheKey("storage", strconv.FormatUint(id, 10))
		st, ok := cached[key].(*data.Storage)

		var s *data.Storage
		if ok {
			s = st
		} else {
			ctx.Debugf("Cache MISS on key '%s'", key)
			misses++
			s, err = self.Lookup(id)
			if err != nil {
				return nil, err
			}
		}
		ret = append(ret, s)
	}

	ctx.Debugf("Loaded %d storages (cache misses = %d)", len(ret), misses)
	return ret, nil
}

func (self *Storage) LoadInCluster(clusterId uint64) ([]*data.Storage, error) {
	ctx := self.Ctx()

	closer := ctx.LogMark("[Storage.LoadInCluster]")
	defer closer()

	sql := `SELECT id FROM storage WHERE cluster_id = ?`

	list, err := self.LookupFromSql(sql, []interface{}{clusterId})
	if err != nil {
		return nil, err
	}

	ctx.Debugf("Loaded %d storages", len(list))
	return list, nil
}

func (self *Storage) ReadableModes(isRepair bool) []int {
	var modes []int
	if isRepair {
		modes = stf.READABLE_MODES_ON_REPAIR
	} else {
		modes = stf.READABLE_MODES
	}
	return modes
}

func (self *Storage) WritableModes(isRepair bool) []int {
	var modes []int
	if isRepair {
		modes = stf.WRITABLE_MODES_ON_REPAIR
	} else {
		modes = stf.WRITABLE_MODES
	}
	return modes
}

func IsModeIn(s *data.Storage, modes []int) bool {
	for _, mode := range modes {
		if s.Mode == mode {
			return true
		}
	}
	return false
}

func (self *Storage) IsReadable(s *data.Storage, isRepair bool) bool {
	return IsModeIn(s, self.ReadableModes(isRepair))
}

func (self *Storage) IsWritable(s *data.Storage, isRepair bool) bool {
	return IsModeIn(s, self.WritableModes(isRepair))
}

func (self *Storage) LoadWritable(clusterId uint64, isRepair bool) ([]*data.Storage, error) {
	ctx := self.Ctx()

	closer := ctx.LogMark("[Storage.LoadWritable]")
	defer closer()

	placeholders := []string{}
	binds := []interface{}{clusterId}
	modes := self.WritableModes(isRepair)

	ctx.Debugf("Repair flag is '%v', using %+v for modes", isRepair, modes)

	for _, v := range modes {
		binds = append(binds, v)
		placeholders = append(placeholders, "?")
	}

	sql := fmt.Sprintf(
		"SELECT id FROM storage WHERE cluster_id = ? AND mode IN (%s)",
		strings.Join(placeholders, ", "),
	)

	list, err := self.LookupFromSql(sql, binds)
	if err != nil {
		return nil, err
	}

	ctx.Debugf("Loaded %d storages", len(list))
	return list, nil
}
