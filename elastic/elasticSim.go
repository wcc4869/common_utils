package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/olivere/elastic/v7"
	"log"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

type EsClient struct {
	//client       *elastic.Client
	url          string //服务地址,多个地址用,隔开
	username     string
	password     string
	Address      []string
	Pool         chan *elastic.Client
	EcSize       int  //客户端个数
	setSniff     bool //用于关闭 Sniff
	header       http.Header
	lastTime     int64
	lastTimeLock sync.Mutex
	ntimeout     int
}

//InitElasticSize 初始化连接池
func (e EsClient) InitElasticSize() {
	e.Pool = make(chan *elastic.Client, e.EcSize)
	for _, s := range strings.Split(e.url, ",") {
		e.Address = append(e.Address, s)
	}
	for i := 0; i < e.EcSize; i++ {
		client, err := elastic.NewClient(
			elastic.SetURL(e.url),        //elastic 服务地址,多个地址用,隔开
			elastic.SetSniff(e.setSniff), //用于关闭 Sniff
			elastic.SetBasicAuth(e.username, e.password),
			// 设置错误日志输出
			elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)))
		if err != nil {
			log.Fatalln("Failed to create elastic err:", err.Error())
		}
		e.Pool <- client
	}
}

//GetEsConn 获取连接
func (e *EsClient) GetEsConn() *elastic.Client {
	select {
	case c := <-e.Pool:
		if c == nil || !c.IsRunning() {
			log.Println("new esclient.", len(e.Pool))
			client, err := elastic.NewClient(elastic.SetURL(e.Address...),
				elastic.SetSniff(e.setSniff), elastic.SetBasicAuth(e.username, e.password))
			if err == nil && client.IsRunning() {
				return client
			}
		}
		return c
	case <-time.After(time.Second * 4):
		//超时
		e.ntimeout++
		e.lastTimeLock.Lock()
		defer e.lastTimeLock.Unlock()
		//12秒后允许创建链接
		ct := time.Now().Unix() - e.lastTime
		if ct > 12 {
			e.lastTime = time.Now().Unix()
			log.Println("add client..", len(e.Pool))
			c, _ := elastic.NewClient(elastic.SetURL(e.Address...), elastic.SetSniff(e.setSniff), elastic.SetBasicAuth(e.username, e.password))
			go func() {
				for i := 0; i < 2; i++ {
					client, _ := elastic.NewClient(elastic.SetURL(e.Address...), elastic.SetSniff(e.setSniff), elastic.SetBasicAuth(e.username, e.password))
					e.Pool <- client
				}
			}()
			return c
		}
		return nil
	}
}

//DestroyEsConn 关闭连接
func (e *EsClient) DestroyEsConn(client *elastic.Client) {
	select {
	case e.Pool <- client:
		break
	case <-time.After(time.Second * 1):
		if client != nil {
			client.Stop()
		}
		client = nil
	}
}

////InitEsClient 初始化客户端
//func (ec EsClient) InitEsClient() {
//	var err error
//	client, err := elastic.NewClient(
//		elastic.SetURL(ec.url),        //elastic 服务地址,多个地址用,隔开
//		elastic.SetSniff(ec.setSniff), //用于关闭 Sniff
//		elastic.SetBasicAuth(ec.username, ec.password),
//		elastic.SetHeaders(ec.header),
//		// 设置错误日志输出
//		elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)))
//
//	if err != nil {
//		log.Fatalln("Failed to create elastic err:", err.Error())
//
//	}
//
//	info, code, err := client.Ping(ec.url).Do(context.Background())
//	if err != nil {
//		panic(err)
//	}
//	ec.client = client
//	fmt.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)
//
//}

//BatchSave 批量存入es
func (ec EsClient) BatchSave(c context.Context, index string, obj *[]map[string]interface{}, isDelBefore bool) error {
	client := ec.GetEsConn()
	defer ec.DestroyEsConn(client)

	if client != nil {
		req := client.Bulk().Index(index)
		for _, v := range *obj {
			if isDelBefore {
				req = req.Add(elastic.NewBulkIndexRequest().Id(fmt.Sprintf("%v", v["_id"])).Doc(v))
			} else {
				req = req.Add(elastic.NewBulkIndexRequest().Doc(v))
			}
		}

		res, err := req.Do(c)
		if err != nil {
			return err
		}
		// 任何子请求失败，该 `errors` 标志被设置为 `true` ，并且在相应的请求报告出错误明细
		// 所以如果没有出错，说明全部成功了，直接返回即可
		if !res.Errors {
			return nil
		}
		for _, it := range res.Failed() {
			if it.Error == nil {
				continue
			}
			return &elastic.Error{
				Status:  it.Status,
				Details: it.Error,
			}
		}
	}

	return nil
}

//CreateIndex 创建索引
func (ec EsClient) CreateIndex(c context.Context, index string, mapping string) (err error) {
	client := ec.GetEsConn()
	defer ec.DestroyEsConn(client)

	exist, err := client.IndexExists(index).Do(c)
	if err != nil {
		log.Fatalln("IndexExists err =>", err)
	}
	if exist {
		return errors.New("index  already exist")
	}

	_, err = client.CreateIndex(index).BodyString(mapping).Do(c)
	if err != nil {
		log.Fatalln("CreateIndex:err ", err)
	}

	return
}

//DeleteIndex DeleteIndex
func (ec EsClient) DeleteIndex(c context.Context, index string) error {
	client := ec.GetEsConn()
	defer ec.DestroyEsConn(client)
	_, err := client.DeleteIndex(index).Do(c)
	return err
}

/**
 mapping1 = `
{
  "properties": {
    "sex": {
      "type": "text"
    }
  }
}
}`
*/

//AddMapField  index 添加 mapping 新字段
func (ec EsClient) AddMapField(c context.Context, index string, mapping string) (err error) {
	client := ec.GetEsConn()
	defer ec.DestroyEsConn(client)
	_, err = client.PutMapping().Index(index).BodyString(mapping).Do(c)
	return
}

/**
id为字符串, 创建一条此id的数据或覆盖已有此id的记录
data为结构体或map, 当然结构需要跟索引的mapping类型保持一致
*/

//InsertOrUpdateByString 根据字符串创建或者更新单个数据
func (ec EsClient) InsertOrUpdateByString(c context.Context, index string, doc string, id string) (rid string, err error) {
	client := ec.GetEsConn()
	defer ec.DestroyEsConn(client)
	if id != "" {
		rs, err := client.Index().Index(index).BodyString(doc).Id(id).Do(c)
		return rs.Id, err
	} else {
		rs, err := client.Index().Index(index).BodyString(doc).Do(c)
		return rs.Id, err
	}
}

//InsertOrUpdateByJson 根据结构体、map创建或者更新单个数据
func (ec EsClient) InsertOrUpdateByJson(c context.Context, index string, doc interface{}, id string) (err error) {
	client := ec.GetEsConn()
	defer ec.DestroyEsConn(client)
	if id != "" {
		_, err = client.Index().Index(index).BodyJson(doc).Id(id).Do(c)
	} else {
		_, err = client.Index().Index(index).BodyJson(doc).Do(c)
	}
	return
}

//GetNoLimit 获取所有数据
func (ec EsClient) GetNoLimit(c context.Context, index string, query string) *[]map[string]interface{} {
	client := ec.GetEsConn()
	defer ec.DestroyEsConn(client)
	var res []map[string]interface{}
	if client != nil {
		defer func() {
			if r := recover(); r != nil {
				log.Println("[E]", r)
				for skip := 1; ; skip++ {
					_, file, line, ok := runtime.Caller(skip)
					if !ok {
						break
					}
					go log.Printf("%v,%v\n", file, line)
				}
			}
		}()
		searchResult, err := client.Search().Index(index).Source(query).Do(c)
		if err != nil {
			log.Println("从ES查询出错", err.Error())
			return nil
		}

		if searchResult.Hits != nil {
			resNum := len(searchResult.Hits.Hits)
			res = make([]map[string]interface{}, resNum)
			for i, hit := range searchResult.Hits.Hits {
				json.Unmarshal(hit.Source, &res[i])
			}
		}
	}
	return &res

}

//GetByIdField GetByIdField
func (ec EsClient) GetByIdField(c context.Context, index, id, fields string) *map[string]interface{} {
	client := ec.GetEsConn()
	defer ec.DestroyEsConn(client)
	if client != nil {
		defer func() {
			if r := recover(); r != nil {
				log.Println("[E]", r)
				for skip := 1; ; skip++ {
					_, file, line, ok := runtime.Caller(skip)
					if !ok {
						break
					}
					go log.Printf("%v,%v\n", file, line)
				}
			}
		}()
		query := `{"query":{"term":{"_id":"` + id + `"}}`
		if len(fields) > 0 {
			query = query + `,"_source":[` + fields + `]`
		}
		query = query + "}"
		searchResult, err := client.Search().Index(index).Source(query).Do(c)
		if err != nil {
			log.Println("从ES查询出错", err.Error())
			return nil
		}
		var res map[string]interface{}
		if searchResult.Hits != nil {
			resNum := len(searchResult.Hits.Hits)
			if resNum == 1 {
				res = make(map[string]interface{})
				for _, hit := range searchResult.Hits.Hits {
					json.Unmarshal(hit.Source, &res)
				}
				return &res
			}
		}
	}
	return nil
}

//GetDocs GetDocs
func (ec EsClient) GetDocs(c context.Context, index string, from int, size int, query elastic.Query) *map[string]interface{} {
	client := ec.GetEsConn()
	defer ec.DestroyEsConn(client)

	searchResult, err := client.Search().Index(index).From(from).Size(size).FetchSourceContext(
		elastic.NewFetchSourceContext(true).Include("id", "name")).Query(query).Sort("id", true).Do(c)

	if err != nil {
		log.Println("从ES查询出错", err.Error())
		return nil
	}
	var res map[string]interface{}
	if searchResult.Hits != nil {
		resNum := len(searchResult.Hits.Hits)
		if resNum == 1 {
			res = make(map[string]interface{})
			for _, hit := range searchResult.Hits.Hits {
				json.Unmarshal(hit.Source, &res)
			}
			return &res
		}
	}
	return nil
}

//Count Count
func (e *EsClient) Count(c context.Context, index string, query interface{}) (int64, error) {
	client := e.GetEsConn()
	defer e.DestroyEsConn(client)
	if client != nil {
		defer func() {
			if r := recover(); r != nil {
				log.Println("[E]", r)
				for skip := 1; ; skip++ {
					_, file, line, ok := runtime.Caller(skip)
					if !ok {
						break
					}
					go log.Printf("%v,%v\n", file, line)
				}
			}
		}()
		var qq elastic.Query
		if qi, ok2 := query.(elastic.Query); ok2 {
			qq = qi
		}
		n, err := client.Count(index).Query(qq).Do(c)
		if err != nil {
			log.Println("统计出错", err.Error())
		}

		return n, err
	}
	return 0, nil
}

//SetAlias 设置别名
func (ec EsClient) SetAlias(c context.Context, index string, aliasName string) (err error) {
	client := ec.GetEsConn()
	defer ec.DestroyEsConn(client)
	_, err = client.Alias().Add(index, aliasName).Do(c)
	return
}

//GetAlias 获取索引所有别名
func (ec EsClient) GetAlias(c context.Context, index string) (alias []string, err error) {
	client := ec.GetEsConn()
	defer ec.DestroyEsConn(client)
	aliasesResult, err := client.Aliases().Index(index).Do(c)
	if err != nil {
		log.Fatalln("GetAlias:", err)
	}
	if len(aliasesResult.Indices) != 2 {
		fmt.Errorf("expected len(AliasesResult.Indices) = %d; got %d", 2, len(aliasesResult.Indices))
	}

	for indexName, indexDetails := range aliasesResult.Indices {
		if len(indexDetails.Aliases) != 1 {
			fmt.Errorf("expected len(AliasesResult.Indices[%s].Aliases) = %d; got %d", indexName, 1, len(indexDetails.Aliases))
		}

		for _, v := range indexDetails.Aliases {
			alias = append(alias, v.AliasName)
		}
	}

	//fmt.Println("alias =>", alias)
	return
}

/**
map[string]interface
key是更新的ID，value是更新的具体数据
*/

// UpdateBulk 批量修改文档
func (ec *EsClient) UpdateBulk(c context.Context, index string, docs ...[]map[string]interface{}) (err error) {
	client := ec.GetEsConn()
	defer ec.DestroyEsConn(client)
	//bulkService := client.Bulk().Index(index).Refresh("true")
	bulkService := client.Bulk().Index(index).Type("_doc")

	for _, d := range docs {
		id := d[0]["_id"].(string)
		doc := elastic.NewBulkUpdateRequest().Id(id).Doc(d[1])
		bulkService.Add(doc)
	}
	_, err = bulkService.Do(c)
	if err != nil {
		fmt.Printf("UpdateBulk all success err is %v\n", err)
	}
	return
}

// UpsertBulk 批量修改文档（不存在则插入）
func (ec *EsClient) UpsertBulk(ctx context.Context, index string, ids []string, docs []interface{}) error {
	client := ec.GetEsConn()
	defer ec.DestroyEsConn(client)
	bulkService := client.Bulk().Index(index)
	bulkService.Type("bidding")
	for i := range ids {
		doc := elastic.NewBulkUpdateRequest().Id(ids[i]).Doc(docs[i]).Upsert(docs[i])
		bulkService.Add(doc)
	}
	res, err := bulkService.Do(ctx)
	if err != nil {
		return err
	}
	if len(res.Failed()) > 0 {
		return errors.New(res.Failed()[0].Error.Reason)
	}
	return nil
}

//DeleteByID 按照ID删除文档
func (ec EsClient) DeleteByID(c context.Context, index string, id string) (err error) {
	client := ec.GetEsConn()
	defer ec.DestroyEsConn(client)
	if client != nil {
		_, err = client.Delete().Index(index).Id(id).Do(c)
	}
	return err
}

//DeleteByIds 批量删除
func (ec EsClient) DeleteByIds(c context.Context, index string, ids []string) (err error) {
	client := ec.GetEsConn()
	defer ec.DestroyEsConn(client)
	bulkService := client.Bulk().Index(index)
	for i := range ids {
		req := elastic.NewBulkDeleteRequest().Id(ids[i])
		bulkService.Add(req)
	}
	res, err := bulkService.Do(c)
	if err != nil {
		fmt.Printf("DeleteBulk success is %v\n", len(res.Succeeded()))
	}

	return
}
