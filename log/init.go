// +build !linux

package log

func init() {
	if defaultLogger == nil {
		InitLog()
	}
}
