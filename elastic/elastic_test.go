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
		URL:    "http://192.168.3.241:9205,http://192.168.3.149:9200",
		EcSize: 2,
	}
	esc.InitElasticSize()

}
func TestEsClient_CreateIndex(t *testing.T) {
	ctx := context.Background()
	index := "wcc_test"
	mapping := `{
    "mappings": {
        "properties": {
            "service": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "ignore_above": 256,
                        "type": "keyword"
                    }
                }
            },
            "one": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "ignore_above": 256,
                        "type": "keyword"
                    }
                }
            },
            "type": {
                "type": "long"
            },
            "three": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "ignore_above": 256,
                        "type": "keyword"
                    }
                }
            },
            "two": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "ignore_above": 256,
                        "type": "keyword"
                    }
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
	err := esc.BatchSave(context.Background(), "wcc_test", &data, false)
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

	id, err := esc.InsertOrUpdateByString(context.Background(), "test_v1", da, "")
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
	//query := elastic.NewBoolQuery().NewBoolQuery()
	//query.Must(elastic.NewTermsQuery("one", []string{"one", "分类1"}))
	//	query := `{
	//   "bool": {
	//       "must": {
	//           "term": {
	//               "one": "one"
	//           }
	//       }
	//   }
	//}`
	count, err := esc.Count(context.Background(), "bidding_v2", nil)
	if err != nil {
		fmt.Println(err)
	}
	assert.Nil(t, err)
	fmt.Println(count)
}

func TestEsClient_AliasName(t *testing.T) {
	err := esc.SetAlias(context.Background(), "test_v1", "test")
	assert.Nil(t, err)
}

func TestEsClient_GetAlias(t *testing.T) {
	names, err := esc.GetAlias(context.Background(), "test_v1")
	fmt.Println("names =>", names)

	assert.Nil(t, err)
}

func TestEsClient_DeleteIndex(t *testing.T) {
	err := esc.DeleteIndex(context.Background(), "test")
	assert.Nil(t, err)
}

func TestEsClient_GetByIdField(t *testing.T) {
	res := esc.GetByIdField(context.Background(), "test_v1", "mPJokYYBS4dWo9RAwsXA", "")
	fmt.Printf("%+v \n", res)
}

func TestEsClient_GetDocs(t *testing.T) {
	res := esc.GetDocs(context.Background(), "test_v1", 0, 1, nil)
	fmt.Printf("%+v \n", res)
}
func TestEsClient_ExistsIndex(t *testing.T) {
	ex, err := esc.ExistsIndex(context.Background(), "test_v1")
	assert.Nil(t, err)
	assert.Equal(t, true, ex)
}

func TestEsClient_GetMapping(t *testing.T) {
	ma, err := esc.GetMapping(context.Background(), "qyxy_v1")
	assert.Nil(t, err)
	fmt.Printf("%+v \n", ma)
}

func TestEsClient_Random(t *testing.T) {
	fmt.Println(esc.Address)
	res, err := esc.Random(context.Background(), "bidding_v2", 2, nil)
	assert.Nil(t, err)
	fmt.Printf("%+v \n", res)
}

func TestEsClient_GetNoLimit(t *testing.T) {
	qyer := `{
  "query": {
    "bool": {
      "must_not": [
        {
          "term": {
            "company_type": "个体工商户"
          }
        }
      ],
      "should": [
        {
          "multi_match": {
            "query": "康宁",
            "type": "phrase",
            "fields": [
              "name"
            ]
          }
        },
        {
          "multi_match": {
            "query": "corning",
            "type": "phrase",
            "fields": [
              "website_url",
              "company_email"
            ]
          }
        }
      ]
    }
  },
  "size": 100,
  "_source": [
    "company_name",
    "website_url",
    "company_email",
    "company_type"
  ]
}`
	res := esc.GetNoLimit(context.Background(), "qyxy_v1", qyer)
	fmt.Println(len(*res))
}
