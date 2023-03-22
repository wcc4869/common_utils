package mongodb

import (
	"github.com/stretchr/testify/assert"
	"github.com/wcc4869/common_utils/log"
	"go.mongodb.org/mongo-driver/bson"
	"testing"
	"time"
)

var Mgo MongodbSim

func init() {
	Mgo = MongodbSim{
		MongodbAddr: "127.0.0.1:27017",
		//MongodbAddr: "172.17.145.163:27083",
		DbName: "wcc",
		Size:   10,
		//UserName:    "SJZY_RWbid_ES",
		//Password:    "SJZY@B4i4D5e6S",
	}
	Mgo.InitPool()
}
func TestBluk_Insert(t *testing.T) {

}

//func TestGetRandomData(t *testing.T) {
//	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://localhost:27017"))
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.Disconnect(context.Background())
//
//	start, _ := time.Parse("2006-01-02 15:04:05", "2023-03-13 00:00:00")
//	end, _ := time.Parse("2006-01-02 15:04:05", "2023-03-13 23:00:00")
//
//	q := bson.M{
//		"comeintime": map[string]interface{}{
//			"$gte": start.Unix(),
//			"$lte": end.Unix(),
//		},
//	}
//
//	//collection := client.Database("wcc").Collection("bidding_wcc_top_empty_0301")
//	results, err := GetRandomData(client, "wcc", "bidding_wcc_top_empty_03129", 10, q)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	assert.Equal(t, 10, len(results))
//
//}

func TestGetRandomData(t *testing.T) {
	start, _ := time.Parse("2006-01-02 15:04:05", "2023-03-13 00:00:00")
	end, _ := time.Parse("2006-01-02 15:04:05", "2023-03-13 23:00:00")

	q := bson.M{
		"comeintime": map[string]interface{}{
			"$gte": start.Unix(),
			"$lte": end.Unix(),
		},
	}
	results, err := Mgo.GetRandomData("bidding_wcc_top_empty_03129", q, 10)
	if err != nil {
		log.Fatal("GetRandomData", err)
	}

	assert.Equal(t, 10, len(results))
}
