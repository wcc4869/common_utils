package elastic

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

var esc EsClient

func init() {
	esc = EsClient{
		url:    "http://192.168.3.206:9800",
		EcSize: 2,
	}
	esc.InitElasticSize()
}
func TestEsClient_CreateIndex(t *testing.T) {
	ctx := context.Background()
	index := "qyxy_v1"
	mapping := `{
"settings": {
  "index": {
    "analysis": {
      "analyzer": {
        "my_ngram_title": {
          "filter": [
            "lowercase"
          ],
          "tokenizer": "my_ngram_title"
        },
        "my_ngram": {
          "filter": [
            "lowercase"
          ],
          "tokenizer": "my_ngram"
        },
        "douhao": {
          "type": "pattern",
          "pattern": ","
        },
        "p_ngram": {
          "filter": [
            "lowercase"
          ],
          "tokenizer": "p_ngram"
        }
      },
      "tokenizer": {
        "my_ngram_title": {
          "token_chars": [
            "letter",
            "digit",
            "punctuation",
            "symbol"
          ],
          "min_gram": "1",
          "type": "nGram",
          "max_gram": "1"
        },
        "my_ngram": {
          "type": "nGram",
          "min_gram": "2",
          "max_gram": "2"
        },
        "p_ngram": {
          "token_chars": [
            "letter",
            "digit",
            "symbol"
          ],
          "min_gram": "2",
          "type": "nGram",
          "max_gram": "2"
        }
      }
    },
    "number_of_shards": "15",
    "number_of_replicas": "0"
  }
},
"mappings": {
  "qyxy": {
    "transform": {
      "lang": "groovy",
      "script": "ctx._source[\"id\"] = ctx._source[\"_id\"]"
    },
    "_all": {
      "enabled": false
    },
    "_id": {
      "path": "_id"
    },
    "properties": {
      "name": {
        "analyzer": "my_ngram",
        "type": "string",
        "fields": {
          "ent_name": {
            "index": "not_analyzed",
            "type": "string"
          },
          "ent_ikname": {
            "analyzer": "ik_smart",
            "type": "string"
          }
        }
      },
      "company_name": {
        "index": "not_analyzed",
        "type": "string"
      },
      "history_name": {
        "analyzer": "my_ngram",
        "type": "string",
        "fields": {
          "hname": {
            "index": "not_analyzed",
            "type": "string"
          }
        }
      },
      "company_code": {
        "index": "not_analyzed",
        "type": "string"
      },
      "tax_code": {
        "index": "not_analyzed",
        "type": "string"
      },
      "credit_no": {
        "index": "not_analyzed",
        "type": "string"
      },
      "org_code": {
        "index": "not_analyzed",
        "type": "string"
      },
      "company_area": {
        "index": "not_analyzed",
        "type": "string"
      },
      "company_city": {
        "index": "not_analyzed",
        "type": "string"
      },
      "company_district": {
        "index": "not_analyzed",
        "type": "string"
      },
      "company_type_old": {
        "analyzer": "my_ngram_title",
        "type": "string"
      },
      "company_type": {
        "index": "not_analyzed",
        "type": "string"
      },
      "company_type_int": {
        "type": "integer"
      },
      "legal_person": {
        "index": "not_analyzed",
        "type": "string"
      },
      "establish_date": {
        "type": "long"
      },
      "lastupdatetime": {
        "index": "not_analyzed",
        "type": "string"
      },
      "capital": {
        "type": "double"
      },
      "currency": {
        "index": "not_analyzed",
        "type": "string"
      },
      "operation_startdate": {
        "index": "not_analyzed",
        "type": "string"
      },
      "operation_enddate": {
        "index": "not_analyzed",
        "type": "string"
      },
      "authority": {
        "index": "not_analyzed",
        "type": "string"
      },
      "issue_date": {
        "index": "not_analyzed",
        "type": "string"
      },
      "company_status": {
        "index": "not_analyzed",
        "type": "string"
      },
      "company_address": {
        "index": "not_analyzed",
        "type": "string"
      },
      "business_scope": {
        "analyzer": "my_ngram",
        "type": "string"
      },
      "cancel_date": {
        "index": "not_analyzed",
        "type": "string"
      },
      "revoke_date": {
        "index": "not_analyzed",
        "type": "string"
      },
      "company_phone": {
        "index": "not_analyzed",
        "type": "string"
      },
      "company_email": {
        "index": "not_analyzed",
        "type": "string"
      },
      "stock_name": {
        "analyzer": "douhao",
        "type": "string"
      },
      "partners": {
        "properties": {
          "identify_no": {
            "index": "not_analyzed",
            "type": "string"
          },
          "stock_type": {
            "index": "not_analyzed",
            "type": "string"
          },
          "stock_name": {
            "index": "not_analyzed",
            "type": "string"
          },
          "identify_type": {
            "index": "not_analyzed",
            "type": "string"
          },
          "stock_capital": {
            "type": "double"
          },
          "stock_realcapital": {
            "type": "double"
          }
        }
      },
      "updatetime": {
        "type": "long"
      },
      "bid_projectname": {
        "analyzer": "p_ngram",
        "type": "string"
      },
      "bid_purchasing": {
        "analyzer": "p_ngram",
        "type": "string"
      },
      "search_type": {
        "index": "not_analyzed",
        "type": "string"
      },
      "employees": {
        "properties": {
          "employee_name": {
            "index": "not_analyzed",
            "type": "string"
          },
          "position": {
            "index": "not_analyzed",
            "type": "string"
          }
        }
      },
      "employee_name": {
        "analyzer": "douhao",
        "type": "string"
      },
      "bid_area": {
        "analyzer": "p_ngram",
        "type": "string"
      },
      "bid_unittype": {
        "analyzer": "douhao",
        "type": "string"
      },
      "bid_contracttype": {
        "analyzer": "douhao",
        "type": "string"
      },
      "company_shortname": {
        "index": "not_analyzed",
        "type": "string"
      },
      "tag_business": {
        "index": "not_analyzed",
        "type": "string"
      },
      "cancel_date_unix": {
        "type": "long"
      },
      "employee_num": {
        "type": "integer"
      }
    }
  }
}
}`
	err := esc.CreateIndex(ctx, index, mapping)
	assert.Nil(t, err)
}

func TestEsClient_BatchSave(t *testing.T) {
	v1 := map[string]interface{}{
		"one":     "one",
		"two":     "two",
		"three":   "啊加班附件为",
		"service": "测试service1",
		"type":    1,
	}

	v2 := map[string]interface{}{
		"one":     "分类1",
		"two":     "分类招生",
		"three":   "天青色等烟雨",
		"service": "周杰伦的测试歌曲",
		"type":    2,
	}
	data := make([]map[string]interface{}, 0)
	data = append(data, v1, v2)
	err := esc.BatchSave(context.Background(), "test", &data, false)
	assert.Nil(t, err)
}

func TestAddMapField(t *testing.T) {
	mapping1 := `
{
  "properties": {
    "sex": {
      "type": "text"
    }
  }
}
}`

	err := esc.AddMapField(context.Background(), "test", mapping1)
	assert.Nil(t, err)
}

func TestEsClient_InsertOrUpdateByString(t *testing.T) {
	da := `{
"one": "111",
"service": "阿飞巴付完款另外恶气",
"three": "久啊不复刻潜伏期为",
"two": "阿发啊",
"type": 1
}`

	id, err := esc.InsertOrUpdateByString(context.Background(), "bidding_v1", da, "")
	fmt.Println(id)
	assert.Nil(t, err)
}

func TestEsClient_InsertOrUpdateByJson(t *testing.T) {
	//data := map[string]interface{}{
	//	"one":     "安家费",
	//	"two":     "拆迁安置",
	//	"three":   "租房租赁",
	//	"service": "城市规划，建设新农村",
	//	"type":    1,
	//}
	//
	//err := esc.InsertOrUpdateByJson(context.Background(), "test", data, "")
	//assert.Nil(t, err)

	da2 := map[string]interface{}{
		"one":     "城乡结合部",
		"two":     "拆迁安置",
		"three":   "租房租赁",
		"service": "城市规划，建设新农村",
		"type":    1,
	}
	err := esc.InsertOrUpdateByJson(context.Background(), "test", da2, "S-addoUB9leKn2gZK0du")
	assert.Nil(t, err)
}

func TestEsClient_Count(t *testing.T) {
	//query := elastic.NewBoolQuery()
	//query.Must(elastic.NewTermsQuery("one", []string{"one", "分类1"}))
	query := `{
    "bool": {
        "must": {
            "term": {
                "one": "one"
            }
        }
    }
}`
	count, err := esc.Count(context.Background(), "test", query)
	if err != nil {
		fmt.Println(err)
	}
	assert.Nil(t, err)
	fmt.Println(count)
}

func TestEsClient_AliasName(t *testing.T) {
	err := esc.SetAlias(context.Background(), "test", "ta")
	assert.Nil(t, err)
}

func TestEsClient_GetAlias(t *testing.T) {
	names, err := esc.GetAlias(context.Background(), "test")
	fmt.Println("names =>", names)

	assert.Nil(t, err)
}

func TestEsClient_DeleteIndex(t *testing.T) {
	err := esc.DeleteIndex(context.Background(), "test")
	assert.Nil(t, err)
}

func TestEsClient_GetByIdField(t *testing.T) {
	res := esc.GetByIdField(context.Background(), "bidding_v1", "5b8cdaeba5cb26b9b751de7d", "")
	fmt.Printf("%+v \n", res)
}
