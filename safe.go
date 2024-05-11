package etcdseq

import "runtime/debug"

func (l *EtcdSeq) runSafe(fn func()) {
	if p := recover(); p != nil {
		l.logger.Errorf("%+v\n%s", p, debug.Stack())
	}

	fn()
}

func (l *EtcdSeq) goSafe(fn func()) {
	l.wg.Add(1)
	go l.runSafe(func() {
		defer l.wg.Done()
		fn()
	})
}
