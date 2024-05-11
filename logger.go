package etcdseq

import "log"

type Logger interface {
	Errorf(format string, args ...interface{})
}

type stdLogger struct{}

func (stdLogger) Errorf(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}
