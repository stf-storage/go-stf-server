package api

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"github.com/stf-storage/go-stf-server"
	"github.com/stf-storage/go-stf-server/data"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type Entity struct {
	*BaseApi
}

func NewEntity(ctx ContextWithApi) *Entity {
	return &Entity{&BaseApi{ctx}}
}

func (self *Entity) Lookup(objectId uint64, storageId uint64) (*data.Entity, error) {
	ctx := self.Ctx()

	closer := ctx.LogMark("[Entity.Lookup]")
	defer closer()

	tx, err := ctx.Txn()

	row := tx.QueryRow(
		"SELECT status FROM entity WHERE object_id = ? AND storage_id = ?",
		objectId,
		storageId,
	)

	e := data.Entity{objectId, storageId, 0}
	err = row.Scan(&e.Status)
	if err != nil {
		ctx.Debugf(
			"Failed to execute query (Entity.Lookup [%d, %d]): %s",
			objectId,
			storageId,
			err,
		)
		return nil, err
	}

	ctx.Debugf("Successfully loaded entity for object %d storage %d", objectId, storageId)
	return &e, nil
}

func (self *Entity) LookupFromRows(rows *sql.Rows) ([]*data.Entity, error) {
	ctx := self.Ctx()

	closer := ctx.LogMark("[Entity.LookupFromRows]")
	defer closer()

	var list []*data.Entity
	for rows.Next() {
		e := &data.Entity{}
		err := rows.Scan(&e.ObjectId, &e.StorageId, &e.Status)
		if err != nil {
			return nil, err
		}
		list = append(list, e)
	}

	ctx.Debugf("Loaded %d entities", len(list))
	return list, nil
}

func (self *Entity) LookupForObjectNotInCluster(objectId uint64, clusterId uint64) ([]*data.Entity, error) {
	ctx := self.Ctx()

	closer := ctx.LogMark("[Entity.LookupForObjectNotInCluster]")
	defer closer()

	ctx.Debugf("Looking for entities for object %d", objectId)

	tx, err := ctx.Txn()

	rows, err := tx.Query(`
SELECT e.object_id, e.storage_id, e.status
  FROM entity e JOIN storage s ON e.storage_id = s.id
  WHERE object_id = ? AND s.cluster_id != ?
`,
		objectId,
		clusterId,
	)
	if err != nil {
		return nil, err
	}

	list, err := self.LookupFromRows(rows)
	if err != nil {
		return nil, err
	}

	ctx.Debugf(
		"Loaded %d entities for object %d (except cluster %d)",
		len(list),
		objectId,
		clusterId,
	)
	return list, nil
}

func (self *Entity) LookupForObject(objectId uint64) ([]*data.Entity, error) {
	ctx := self.Ctx()

	closer := ctx.LogMark("[Entity.LookupForObject]")
	defer closer()

	ctx.Debugf("Looking for entities for object %d", objectId)

	tx, err := ctx.Txn()

	rows, err := tx.Query(
		`SELECT e.object_id, e.storage_id, e.status FROM entity e WHERE object_id = ?`,
		objectId,
	)
	if err != nil {
		return nil, err
	}

	list, err := self.LookupFromRows(rows)
	if err != nil {
		return nil, err
	}

	ctx.Debugf("Loaded %d entities for object %d", len(list), objectId)
	return list, nil
}

func (self *Entity) Create(
	objectId uint64,
	storageId uint64,
) error {
	ctx := self.Ctx()

	closer := ctx.LogMark("[Entity.Create]")
	defer closer()

	tx, err := ctx.Txn()
	if err != nil {
		return err
	}

	_, err = tx.Exec("INSERT INTO entity (object_id, storage_id, status, created_at) VALUES (?, ?, 1, UNIX_TIMESTAMP())", objectId, storageId)
	if err != nil {
		ctx.Debugf("Failed to execute query: %s", err)
		return err
	}

	ctx.Debugf("Created entity entry for '%d', '%d'", objectId, storageId)
	return nil
}

func (self *Entity) FetchContent(o *data.Object, s *data.Storage, isRepair bool) (io.ReadCloser, error) {
	ctx := self.Ctx()

	closer := ctx.LogMark("[Entity.FetchContent]")
	defer closer()

	storageApi := ctx.StorageApi()
	if !storageApi.IsReadable(s, isRepair) {
		return nil, errors.New(
			fmt.Sprintf(
				"Storage %d is not readable",
				s.Id,
			),
		)
	}

	return self.FetchContentNocheck(o, s, isRepair)
}

func (self *Entity) FetchContentNocheck(
	o *data.Object,
	s *data.Storage,
	isRepair bool,
) (io.ReadCloser, error) {

	ctx := self.Ctx()

	closer := ctx.LogMark("[Entity.FetchContentNocheck]")
	defer closer()

	ctx.Debugf(
		"Fetching content from storage %d",
		s.Id,
	)

	client := &http.Client{}

	uri := strings.Join([]string{s.Uri, o.InternalName}, "/")
	ctx.Debugf(
		"Sending GET %s (object = %d, storage = %d)",
		uri,
		o.Id,
		s.Id,
	)

	// XXX Original perl version used to optimize the content fetch
	// here by writing the content into the file system in chunks.
	// Does go need/have such a mechanism?
	resp, err := client.Get(uri)
	if err != nil {
		return nil, err
	}

	var okStr string
	if resp.StatusCode == 200 {
		okStr = "OK"
	} else {
		okStr = "FAIL"
	}
	ctx.Debugf(
		"        GET %s was %s (%s)",
		uri,
		okStr,
		resp.StatusCode,
	)

	if resp.ContentLength != o.Size {
		ctx.Debugf(
			"Fetched content size for object %d does not match registered size?! (got %d, expected %d)",
			o.Id,
			resp.ContentLength,
			o.Size,
		)
		return nil, errors.New("Content size mismatch")
	}

	ctx.Debugf(
		"Success fetching %s (object = %d, storage = %d)",
		uri,
		o.Id,
		s.Id,
	)

	return resp.Body, nil
}

func (self *Entity) FetchContentFromStorageIds(o *data.Object, list []uint64, isRepair bool) (io.ReadCloser, error) {
	ctx := self.Ctx()

	closer := ctx.LogMark("[Entity.FetchContentFromStorageIds]")
	defer closer()

	storageApi := ctx.StorageApi()
	storages, err := storageApi.LookupMulti(list)
	if err != nil {
		return nil, err
	}

	for _, s := range storages {
		content, err := self.FetchContentNocheck(o, s, isRepair)
		if err == nil {
			return content, nil
		}
	}

	return nil, errors.New("Failed to fetch any content")
}

func (self *Entity) FetchContentFromAll(o *data.Object, isRepair bool) (io.ReadCloser, error) {
	ctx := self.Ctx()

	closer := ctx.LogMark("[Entity.FetchContentFromAll]")
	defer closer()

	sql := "SELECT s.id FROM storage s ORDER BY rand()"
	tx, err := ctx.Txn()
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(sql)

	var list []uint64
	for rows.Next() {
		var sid uint64
		err = rows.Scan(&sid)
		if err != nil {
			return nil, err
		}

		list = append(list, sid)
	}

	return self.FetchContentFromStorageIds(o, list, isRepair)
}

func (self *Entity) FetchContentFromAny(o *data.Object, isRepair bool) (io.ReadCloser, error) {
	ctx := self.Ctx()

	closer := ctx.LogMark("[Entity.FetchContentFromAny]")
	defer closer()

	sql := `
SELECT s.id
  FROM storage s JOIN entity e ON s.id = e.storage_id
  WHERE s.mode IN (?, ?) AND e.object_id = ?
  ORDER BY rand()
`

	tx, err := ctx.Txn()
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(sql, stf.STORAGE_MODE_READ_ONLY, stf.STORAGE_MODE_READ_WRITE, o.Id)
	if err != nil {
		return nil, err
	}

	var list []uint64
	for rows.Next() {
		var sid uint64
		err = rows.Scan(&sid)
		if err != nil {
			return nil, err
		}

		list = append(list, sid)
	}

	return self.FetchContentFromStorageIds(o, list, isRepair)
}

func (self *Entity) Store(
	storageObj *data.Storage,
	objectObj *data.Object,
	input *bytes.Reader,
) error {
	ctx := self.Ctx()

	closer := ctx.LogMark("[Entity.Store]")
	defer closer()

	uri := strings.Join([]string{storageObj.Uri, objectObj.InternalName}, "/")
	cl := input.Len()

	ctx.Debugf("Going to store %d bytes in %s", cl, uri)

	req, err := http.NewRequest("PUT", uri, input)
	if err != nil {
		ctx.Debugf("Failed to create request: %s", err)
		return err
	}

	// XXX Need to check if this vanilla http client is ok
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		ctx.Debugf("Failed to send PUT request to %s (storage = %d): %s", uri, storageObj.Id, err)
		return err
	}

	if resp.StatusCode != 201 {
		err = errors.New(
			fmt.Sprintf(
				"Expected response 201 for PUT request, but did not get it: %s",
				resp.Status,
			),
		)
		ctx.Debugf("Failed to store PUT request to %s (storage = %d): %s", uri, storageObj.Id, err)
		return err
	}

	ctx.Debugf("Successfully stored object in %s", uri)

	err = self.Create(
		objectObj.Id,
		storageObj.Id,
	)

	if err != nil {
		return err
	}

	return nil
}

// Proceed with caution!!!! THIS WILL DELETE THE ENTIRE ENTITY SET!
func (self *Entity) DeleteOrphansForObjectId(objectId uint64) error {
	ctx := self.Ctx()

	closer := ctx.LogMark("[Entity.DeletedOrphasForObjectId]")
	defer closer()

	tx, err := ctx.Txn()
	if err != nil {
		return err
	}

	_, err = tx.Exec("DELETE FROM entity WHERE object_id = ?", objectId)
	return err
}

func (self *Entity) RemoveForDeletedObjectId(objectId uint64) error {
	ctx := self.Ctx()

	closer := ctx.LogMark("[EntityRemoveForDeletedObjectId]")
	defer closer()

	// Find existing entities
	entities, err := self.LookupForObject(objectId)
	if err != nil {
		return err
	}
	for _, e := range entities {
		err = self.RemoveDeleted(e, true)
		if err != nil {
			return err
		}
	}

	return nil
}

func (self *Entity) CheckHealth(o *data.Object, s *data.Storage, isRepair bool) error {
	ctx := self.Ctx()

	closer := ctx.LogMark("[Entity.CheckHealth]")
	defer closer()

	ctx.Debugf("Checking entity health on object %d storage %d", o.Id, s.Id)

	_, err := self.Lookup(o.Id, s.Id)
	if err != nil {
		ctx.Debugf(
			"Entity on storage %d for object %d is not recorded.",
			s.Id,
			o.Id,
		)
		return errors.New(
			fmt.Sprintf(
				"Could not find entity in database: %s",
				err,
			),
		)
	}

	// An entity in TEMPORARILY_DOWN node needs to be treated as alive
	if s.Mode == stf.STORAGE_MODE_TEMPORARILY_DOWN {
		ctx.Debugf(
			"Storage %d is temporarily down. Assuming this is intact.",
			s.Id,
		)
		return nil
	}

	// If the mode is not in a readable state, then we've purposely
	// taken it out of the system, and needs to be repaired. Also,
	// if this were the case, we DO NOT issue an DELETE on the backend,
	// as it most likely will not properly respond.

	storageApi := ctx.StorageApi()
	if !storageApi.IsReadable(s, isRepair) {
		ctx.Debugf(
			"Storage %d is not reable. Adding to invalid list.",
			s.Id,
		)
		return errors.New("Storage is down")
	}

	url := strings.Join([]string{s.Uri, o.InternalName}, "/")
	ctx.Debugf(
		"Going to check %s (object_id = %d, storage_id = %d)",
		url,
		o.Id,
		s.Id,
	)

	client := &http.Client{}
	res, err := client.Get(url)

	var okStr string
	var st int
	if err != nil {
		okStr = "FAIL"
		st = 500
	} else if res.StatusCode != 200 {
		okStr = "FAIL"
		st = res.StatusCode
	} else {
		okStr = "OK"
		st = res.StatusCode
	}

	ctx.Debugf(
		"GET %s was %s (%d)",
		url,
		okStr,
		st,
	)

	if err != nil {
		return errors.New("An error occurred while trying to fetch entity")
	}

	if res.StatusCode != 200 {
		return errors.New(
			fmt.Sprintf(
				"Failed to fetch entity: %s",
				res.Status,
			),
		)
	}

	if res.ContentLength != o.Size {
		ctx.Debugf(
			"Object %d sizes do not match (got %d, expected %d)",
			o.Id,
			res.ContentLength,
			o.Size,
		)
		return errors.New("Object size mismatch")
	}

	return nil
}

func (self *Entity) SetStatus(e *data.Entity, st int) error {
	ctx := self.Ctx()

	closer := ctx.LogMark("[Entity.SetStatus]")
	defer closer()

	tx, err := ctx.Txn()
	if err != nil {
		return err
	}

	_, err = tx.Exec(
		"UPDATE entity SET status = ? WHERE object_id = ? AND storage_id = ?",
		st,
		e.ObjectId,
		e.StorageId,
	)

	if err != nil {
		ctx.Debugf(
			"Failed to set status of entity (object %d storage %d) to %d)",
			e.ObjectId,
			e.StorageId,
			st,
		)
		return err
	}

	ctx.Debugf(
		"Successfully set status of entity (object %d storage %d) to %d)",
		e.ObjectId,
		e.StorageId,
		st,
	)
	return nil
}

func (self *Entity) Delete(objectId uint64, storageId uint64) error {
	ctx := self.Ctx()

	closer := ctx.LogMark("[Entity.Delete]")
	defer closer()

	tx, err := ctx.Txn()
	if err != nil {
		return err
	}

	_, err = tx.Exec(
		"DELETE FROM entity WHERE object_id = ? AND storage_id = ?",
		objectId,
		storageId,
	)

	if err != nil {
		ctx.Debugf(
			"Failed to delete logical entity (object %d, storage %d): %s",
			objectId,
			storageId,
			err,
		)
		return err
	}

	ctx.Debugf(
		"Successfully deleted logical entity (object %d, storage %d)",
		objectId,
		storageId,
	)

	return nil
}

func (self *Entity) Remove(e *data.Entity, isRepair bool) error {
	return self.removeInternal(
		e,
		isRepair,
		false, // useDeletedObject: "no"
	)
}

func (self *Entity) RemoveDeleted(e *data.Entity, isRepair bool) error {
	return self.removeInternal(
		e,
		isRepair,
		true, // useDeletedObject: "yes"
	)
}

func (self *Entity) removeInternal(e *data.Entity, isRepair bool, useDeletedObject bool) error {
	ctx := self.Ctx()

	closer := ctx.LogMark("[Entity.Remove]")
	defer closer()

	self.Delete(e.ObjectId, e.StorageId)

	cache := ctx.Cache()
	cacheKey := cache.CacheKey(
		"storage",
		strconv.FormatUint(e.StorageId, 10),
		"http_accessible",
	)
	var httpAccesibleFlg int64
	err := cache.Get(cacheKey, &httpAccesibleFlg)
	if err == nil && httpAccesibleFlg == -1 {
		ctx.Debugf(
			"Storage %d was previously unaccessible, skipping physical delete",
			e.StorageId,
		)
		return errors.New("Storage is inaccessible (negative cache)")
	}

	storageApi := ctx.StorageApi()
	s, err := storageApi.Lookup(e.StorageId)
	if err != nil {
		return err
	}

	if !storageApi.IsWritable(s, isRepair) {
		ctx.Debugf("Storage %d is not writable (isRepair = %s)", s.Id, isRepair)
		return errors.New("Storage is not writable")
	}

	var internalName string
	if useDeletedObject {
		o, err := ctx.DeletedObjectApi().Lookup(e.ObjectId)
		if err != nil {
			return err
		}
		internalName = o.InternalName
	} else {
		o, err := ctx.ObjectApi().Lookup(e.ObjectId)
		if err != nil {
			return err
		}
		internalName = o.InternalName
	}

	uri := strings.Join([]string{s.Uri, internalName}, "/")
	req, err := http.NewRequest("DELETE", uri, nil)
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		// If you got here, the 'error' is usually error in DNS resolution
		// or connection refused and such. Remember this incident via a
		// negative cache, so that we don't keep on
		cache.Set(cacheKey, -1, 300)
		return err
	}

	switch {
	case res.StatusCode == 404:
		ctx.Debugf("%s was not found while deleting (ignored)", uri)
	case res.StatusCode >= 200 && res.StatusCode < 300:
		ctx.Debugf("Successfully deleted %s", uri)
	default:
		ctx.Debugf("An error occurred while deleting %s: %s", uri, res.Status)
	}

	return nil
}
