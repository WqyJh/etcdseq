package etcdlb

import "runtime/debug"

func (l *EtcdLB) runSafe(fn func()) {
	if p := recover(); p != nil {
		l.logger.Errorf("%+v\n%s", p, debug.Stack())
	}

	fn()
}

func (l *EtcdLB) goSafe(fn func()) {
	l.wg.Add(1)
	go l.runSafe(func() {
		defer l.wg.Done()
		fn()
	})
}
