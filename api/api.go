package api

type BaseApi struct {
	ctx ContextWithApi
}

type ApiHolder interface {
	BucketApi() *Bucket
	DeletedObjectApi() *DeletedObject
	EntityApi() *Entity
	ObjectApi() *Object
	QueueApi() QueueApiInterface
	StorageApi() *Storage
	StorageClusterApi() *StorageCluster
}

func (self *BaseApi) Ctx() ContextWithApi { return self.ctx }
