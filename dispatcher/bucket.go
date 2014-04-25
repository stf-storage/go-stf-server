package dispatcher

import (
	"database/sql"
	"github.com/stf-storage/go-stf-server/api"
	"log"
)

func (self *Dispatcher) CreateBucket(ctx *DispatcherContext, bucketName string, objectName string) *HTTPResponse {
	rollback, err := ctx.TxnBegin()
	if err != nil {
		ctx.Debugf("Failed to start transaction: %s", err)
		return HTTPInternalServerError
	}
	defer rollback()

	closer := ctx.LogMark("[Dispatcher.CreateBucket]")
	defer closer()

	if objectName != "" {
		return &HTTPResponse{Code: 400, Message: "Bad bucket name"}
	}

	bucketApi := ctx.BucketApi()

	id, err := bucketApi.LookupIdByName(bucketName)
	if err == nil { // No error, so we found a bucket
		ctx.Debugf("Bucket '%s' already exists (id = %d)", bucketName, id)
		return HTTPNoContent
	} else if err != sql.ErrNoRows {
		ctx.Debugf("Error while looking up bucket '%s': %s", bucketName, err)
		return HTTPInternalServerError
	}

	// If we got here, it's a new Bucket Create it
	id = self.IdGenerator().CreateId()
	log.Printf("id = %d", id)

	err = bucketApi.Create(
		id,
		bucketName,
	)

	if err != nil {
		ctx.Debugf("Failed to create bucket '%s': %s", bucketName, err)
		return HTTPInternalServerError
	}

	if err = ctx.TxnCommit(); err != nil {
		ctx.Debugf("Failed to commit: %s", err)
	}

	return HTTPCreated
}

func (self *Dispatcher) DeleteBucket(ctx api.ContextWithApi, bucketName string) *HTTPResponse {
	rollback, err := ctx.TxnBegin()
	if err != nil {
		ctx.Debugf("Failed to start transaction: %s", err)
		return HTTPInternalServerError
	}
	defer rollback()

	bucketApi := ctx.BucketApi()
	id, err := bucketApi.LookupIdByName(bucketName)

	if err != nil {
		return &HTTPResponse{Code: 500, Message: "Failed to find bucket"}
	}

	err = bucketApi.MarkForDelete(id)
	if err != nil {
		ctx.Debugf("Failed to delete bucket %s", err)
		return &HTTPResponse{Code: 500, Message: "Failed to delete bucket"}
	}

	if err = ctx.TxnCommit(); err != nil {
		ctx.Debugf("Failed to commit: %s", err)
	} else {
		ctx.Debugf("Deleted bucket '%s' (id = %d)", bucketName, id)
	}

	return HTTPNoContent
}

// MOVE /bucket_name
// X-STF-Move-Destination: /new_name
func (self *Dispatcher) RenameBucket(ctx api.ContextWithApi, bucketName string, dest string) *HTTPResponse {
	return nil
}
