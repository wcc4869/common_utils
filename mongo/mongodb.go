package mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
)

//MongodbSim MongodbSim
type MongodbSim struct {
	MongodbAddr string
	Size        int
	//	MinSize     int
	DbName   string
	C        *mongo.Client
	Ctx      context.Context
	ShortCtx context.Context
	pool     chan bool
	UserName string
	Password string
	ReplSet  string
}

//MgoSess MgoSess
type MgoSess struct {
	db     string
	coll   string
	query  interface{}
	sorts  []string
	fields interface{}
	limit  int64
	skip   int64
	pipe   []map[string]interface{}
	all    interface{}
	M      *MongodbSim
}

type MgoIter struct {
	Cursor *mongo.Cursor
	Ctx    context.Context
}

//NewMgo NewMgo
func NewMgo(addr, db string, size int) *MongodbSim {
	mgo := &MongodbSim{
		MongodbAddr: addr,
		Size:        size,
		DbName:      db,
	}
	mgo.InitPool()
	return mgo
}
