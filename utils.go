package common_utils

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

//containsChinese  识别含有中文
func containsChinese(str string) bool {
	result, _ := regexp.MatchString(`[\x{4e00}-\x{9fa5}]+`, str)
	if result {
		return true
	}

	return false
}

//IsChineseChar 是否含有中文
func IsChineseChar(str string) bool {
	for _, r := range str {
		if unicode.Is(unicode.Scripts["Han"], r) || (regexp.MustCompile("[\u3002\uff1b\uff0c\uff1a\u201c\u201d\uff08\uff09\u3001\uff1f\u300a\u300b]").MatchString(string(r))) {
			return true
		}
	}
	return false
}

// RegTransferSpecialChar 正则表达式特殊字符转义
func RegTransferSpecialChar(reg string) string {
	//`\`必须第一个替换，否后面转义符重复替换
	sp := []string{`\`, "$", "(", ")", "*", "+", ".", "[", "]", "?", "^", "{", "}", "|", "-"}
	for _, v := range sp {
		reg = strings.ReplaceAll(reg, v, `\`+v)
	}
	return reg
}

// IsPassFullNameRule 人员姓名正则校验
func IsPassFullNameRule(val string) bool {
	pattern := `[\p{Han}a-zA-Z0-9_]{1,8}$`
	hzRegexp := regexp.MustCompile(pattern)
	return hzRegexp.MatchString(val)
}

// IsPassPhoneRule 手机号码正则校验
func IsPassPhoneRule(val string) bool {
	pattern := `^1[0-9]{10}$`
	hzRegexp := regexp.MustCompile(pattern)
	return hzRegexp.MatchString(val)
}

// IsPassEmailRule 邮箱正则校验
func IsPassEmailRule(val string) bool {
	pattern := `^\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$`
	hzRegexp := regexp.MustCompile(pattern)
	return hzRegexp.MatchString(val)
}

// IsPassTagNameRule 标签名合法性校验
func IsPassTagNameRule(val string) bool {
	pattern := `^[\p{Han}a-zA-Z]{1,32}$`
	hzRegexp := regexp.MustCompile(pattern)
	return hzRegexp.MatchString(val)
}

func IntAll(num interface{}) int {
	return IntAllDef(num, 0)
}
func IntAllDef(num interface{}, defaultNum int) int {
	if i, ok := num.(int); ok {
		return int(i)
	} else if i0, ok0 := num.(int32); ok0 {
		return int(i0)
	} else if i1, ok1 := num.(float64); ok1 {
		return int(i1)
	} else if i2, ok2 := num.(int64); ok2 {
		return int(i2)
	} else if i3, ok3 := num.(float32); ok3 {
		return int(i3)
	} else if i4, ok4 := num.(string); ok4 {
		in, _ := strconv.Atoi(i4)
		return int(in)
	} else if i5, ok5 := num.(int16); ok5 {
		return int(i5)
	} else if i6, ok6 := num.(int8); ok6 {
		return int(i6)
	} else if i7, ok7 := num.(*big.Int); ok7 {
		in, _ := strconv.Atoi(fmt.Sprint(i7))
		return int(in)
	} else if i8, ok8 := num.(*big.Float); ok8 {
		in, _ := strconv.Atoi(fmt.Sprint(i8))
		return int(in)
	} else {
		return defaultNum
	}
}

func Int64All(num interface{}) int64 {
	if i, ok := num.(int64); ok {
		return int64(i)
	} else if i0, ok0 := num.(int32); ok0 {
		return int64(i0)
	} else if i1, ok1 := num.(float64); ok1 {
		return int64(i1)
	} else if i2, ok2 := num.(int); ok2 {
		return int64(i2)
	} else if i3, ok3 := num.(float32); ok3 {
		return int64(i3)
	} else if i4, ok4 := num.(string); ok4 {
		i64, _ := strconv.ParseInt(i4, 10, 64)
		//in, _ := strconv.Atoi(i4)
		return i64
	} else if i5, ok5 := num.(int16); ok5 {
		return int64(i5)
	} else if i6, ok6 := num.(int8); ok6 {
		return int64(i6)
	} else if i7, ok7 := num.(*big.Int); ok7 {
		in, _ := strconv.ParseInt(fmt.Sprint(i7), 10, 64)
		return int64(in)
	} else if i8, ok8 := num.(*big.Float); ok8 {
		in, _ := strconv.ParseInt(fmt.Sprint(i8), 10, 64)
		return int64(in)
	} else {
		return 0
	}
}

func Float64All(num interface{}) float64 {
	if i, ok := num.(float64); ok {
		return float64(i)
	} else if i0, ok0 := num.(int32); ok0 {
		return float64(i0)
	} else if i1, ok1 := num.(int64); ok1 {
		return float64(i1)
	} else if i2, ok2 := num.(int); ok2 {
		return float64(i2)
	} else if i3, ok3 := num.(float32); ok3 {
		return float64(i3)
	} else if i4, ok4 := num.(string); ok4 {
		in, _ := strconv.ParseFloat(i4, 64)
		return in
	} else if i5, ok5 := num.(int16); ok5 {
		return float64(i5)
	} else if i6, ok6 := num.(int8); ok6 {
		return float64(i6)
	} else if i6, ok6 := num.(uint); ok6 {
		return float64(i6)
	} else if i6, ok6 := num.(uint8); ok6 {
		return float64(i6)
	} else if i6, ok6 := num.(uint16); ok6 {
		return float64(i6)
	} else if i6, ok6 := num.(uint32); ok6 {
		return float64(i6)
	} else if i6, ok6 := num.(uint64); ok6 {
		return float64(i6)
	} else if i7, ok7 := num.(*big.Float); ok7 {
		in, _ := strconv.ParseFloat(fmt.Sprint(i7), 64)
		return float64(in)
	} else if i8, ok8 := num.(*big.Int); ok8 {
		in, _ := strconv.ParseFloat(fmt.Sprint(i8), 64)
		return float64(in)
	} else {
		return 0
	}
}

//根据bsonID转string
func BsonIdToSId(uid interface{}) string {
	if uid == nil {
		return ""
	} else if u, ok := uid.(string); ok {
		return u
	} else if u, ok := uid.(primitive.ObjectID); ok {
		return u.Hex()
	} else {
		return ""
	}
}

func StringTOBsonId(id string) (bid primitive.ObjectID) {
	if id != "" {
		bid, _ = primitive.ObjectIDFromHex(id)
	}
	return
}
