package etcdseq

import "log"

type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

type stdLogger struct{}

func (stdLogger) Debugf(format string, args ...interface{}) {
	log.Printf("[DEBUG] "+format, args...)
}

func (stdLogger) Infof(format string, args ...interface{}) {
	log.Printf("[INFO] "+format, args...)
}

func (stdLogger) Errorf(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}
