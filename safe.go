package etcdseq

import "runtime/debug"

func (l *EtcdSeq) runSafe(fn func()) {
	if p := recover(); p != nil {
		l.logger.Errorf("%+v\n%s", p, debug.Stack())
	}

	fn()
}
