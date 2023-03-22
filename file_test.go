package common_utils

import (
	"github.com/wcc4869/common_utils/log"
	"testing"
)

func TestFileExist(t *testing.T) {
	file := "/tmp/wcc.text"
	rs, err := FileExist(file)
	if err != nil {
		log.Info("TestFileExist", log.Any("FileExist err", err))
	}
	if rs {
		log.Info("/tmp/wcc.text exist")
	} else {
		log.Info("/tmp/wcc.text not exist")
	}
}
