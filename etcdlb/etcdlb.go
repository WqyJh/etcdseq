package etcdlb

import (
	"errors"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const TimeToLive = 10
const indexInvalid = -1

var DefaultInfo = Info{
	Index: indexInvalid,
	Count: 0,
}

type EtcdLBOption func(client *EtcdLB)

type Info struct {
	Index int
	Count int

	revision int64
}

func (i Info) String() string {
	if i.Invalid() {
		return "Invalid"
	}
	return fmt.Sprintf("%d/%d rev:%d", i.Index, i.Count, i.revision)
}

func (i *Info) Reset() {
	i.Index = indexInvalid
	i.Count = 0
}

func (i *Info) Invalid() bool {
	return i.Index == indexInvalid
}

type Handler interface {
	OnChange(info Info)
	OnRevoke()
}

type EtcdLB struct {
	client    *clientv3.Client
	key       string
	keyPrefix string
	value     string
	handler   Handler
	quit      *DoneChan
	logger    Logger

	fullKey string
	info    Info
	lease   clientv3.LeaseID
	wg      sync.WaitGroup
}

func NewEtcdLB(client *clientv3.Client, key string, handler Handler, opts ...EtcdLBOption) *EtcdLB {
	l := &EtcdLB{
		client:    client,
		key:       key,
		value:     key,
		keyPrefix: makeKeyPrefix(key),
		handler:   handler,
		quit:      NewDoneChan(),
		logger:    stdLogger{},
	}
	l.reset()
	for _, opt := range opts {
		opt(l)
	}
	return l
}

func (l *EtcdLB) Start() error {
	err := l.doRegister()
	if err != nil {
		return fmt.Errorf("doRegister: %w", err)
	}
	err = l.keepAliveAsync()
	if err != nil {
		return fmt.Errorf("keepAliveAsync: %w", err)
	}
	l.watchAsync()
	return nil
}

func (l *EtcdLB) Stop() {
	l.quit.Close()
	l.wg.Wait()
}

func (l *EtcdLB) reset() {
	l.info.Reset()
	l.lease = clientv3.NoLease
}

func (l *EtcdLB) doKeepAlive() error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-l.quit.Done():
			return nil
		case <-ticker.C:
			err := l.doRegister()
			if err != nil {
				l.logger.Errorf("doRegister: %+v", err)
				break
			}

			if err := l.keepAliveAsync(); err != nil {
				l.logger.Errorf("keepAliveAsync: %+v", err)
				break
			}

			return nil
		}
	}
}

func (l *EtcdLB) keepAliveAsync() error {
	ch, err := l.client.KeepAlive(l.client.Ctx(), l.lease)
	if err != nil {
		return fmt.Errorf("etcd KeepAlive: %w", err)
	}

	l.goSafe(func() {
		for {
			select {
			case _, ok := <-ch:
				if !ok {
					l.doRevoke()
					if err := l.doKeepAlive(); err != nil {
						l.logger.Errorf("doKeepAlive: %+v", err)
					}
					return
				}
			case <-l.quit.Done():
				l.doRevoke()
				return
			}
		}
	})

	return nil
}

func (l *EtcdLB) watchAsync() {
	l.goSafe(func() {
		if err := l.watch(); err != nil {
			l.logger.Errorf("etcd publisher watch: %+v", err)
		}
	})
}

func (l *EtcdLB) doRegister() (err error) {
	l.lease, err = l.register()
	if err != nil {
		return fmt.Errorf("etcd register: %w", err)
	}
	return l.handleInfoChange()
}

func (l *EtcdLB) register() (clientv3.LeaseID, error) {
	resp, err := l.client.Grant(l.client.Ctx(), TimeToLive)
	if err != nil {
		return clientv3.NoLease, fmt.Errorf("etcd grant: %w", err)
	}
	lease := resp.ID

	l.fullKey = makeEtcdKey(l.key, int64(lease))

	_, err = l.client.Put(l.client.Ctx(), l.fullKey, l.value, clientv3.WithLease(lease))
	if err != nil {
		return clientv3.NoLease, fmt.Errorf("etcd put: %w", err)
	}

	return lease, err
}

func (l *EtcdLB) doRevoke() error {
	err := l.revoke()
	if err != nil {
		l.logger.Errorf("revoke: %+v", err)
		return err
	}
	l.handler.OnRevoke()
	l.reset()
	return nil
}

func (l *EtcdLB) revoke() error {
	_, err := l.client.Revoke(l.client.Ctx(), l.lease)
	if err != nil {
		l.logger.Errorf("etcd Revoke: %+v", err)
	}
	return err
}

func (l *EtcdLB) load() (info Info, err error) {
	resp, err := l.client.Get(l.client.Ctx(), l.key, clientv3.WithPrefix())
	if err != nil {
		return
	}
	index := indexInvalid
	for i, kv := range resp.Kvs {
		if kv.Lease == int64(l.lease) {
			index = i
			break
		}
	}
	if index == indexInvalid {
		return info, fmt.Errorf("lease not found")
	}

	info.Index = index
	info.Count = len(resp.Kvs)
	info.revision = resp.Header.Revision
	return
}

func makeEtcdKey(key string, id int64) string {
	return fmt.Sprintf("%s/%d", key, id)
}

func makeKeyPrefix(key string) string {
	return key + "/"
}

func (l *EtcdLB) watch() error {
	var rch clientv3.WatchChan
	rev := l.info.revision
	if rev == 0 {
		rch = l.client.Watch(clientv3.WithRequireLeader(l.client.Ctx()), l.keyPrefix, clientv3.WithPrefix())
	} else {
		rch = l.client.Watch(clientv3.WithRequireLeader(l.client.Ctx()), l.keyPrefix, clientv3.WithPrefix(), clientv3.WithRev(rev+1))
	}

	for {
		select {
		case <-l.quit.Done():
			return nil
		case resp, ok := <-rch:
			if !ok {
				return errors.New("etcd watch chan has been closed")
			}
			if resp.Canceled {
				return fmt.Errorf("etcd watch chan has been canceled, error: %w", resp.Err())
			}
			if resp.Err() != nil {
				return fmt.Errorf("etcd watch chan has error, error: %w", resp.Err())
			}
			err := l.handleInfoChange()
			if err != nil {
				return fmt.Errorf("etcd handleInfoChange: %w", err)
			}
		}
	}
}

func (l *EtcdLB) handleInfoChange() error {
	info, err := l.load()
	if err != nil {
		return fmt.Errorf("load: %w", err)
	}
	if l.info.Index != info.Index || l.info.Count != info.Count {
		l.handler.OnChange(info)
		l.info = info
	}
	return nil
}

func WithLogger(logger Logger) EtcdLBOption {
	return func(client *EtcdLB) {
		client.logger = logger
	}
}