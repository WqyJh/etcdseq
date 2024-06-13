package etcdseq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const TimeToLive = 10
const indexInvalid = -1
const countInvalid = 0

var DefaultInfo = Info{
	Index: indexInvalid,
	Count: countInvalid,
}

type EtcdSeqOption func(client *EtcdSeq)

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
	i.Count = countInvalid
}

func (i Info) Invalid() bool {
	return i.Index == indexInvalid || i.Count == countInvalid
}

func (i Info) Equal(other Info) bool {
	if i.Invalid() {
		return other.Invalid()
	}
	return i.Index == other.Index && i.Count == other.Count
}

type Handler interface {
	OnChange(info Info)
}

type EtcdSeq struct {
	client    *clientv3.Client
	key       string
	keyPrefix string
	value     string
	handler   Handler
	quit      *DoneChan
	quitWatch *DoneChan
	logger    Logger

	fullKey string
	info    Info
	lease   clientv3.LeaseID
	wg      sync.WaitGroup
	wgWatch sync.WaitGroup
}

func NewEtcdSeq(client *clientv3.Client, key string, handler Handler, opts ...EtcdSeqOption) *EtcdSeq {
	l := &EtcdSeq{
		client:    client,
		key:       key,
		value:     key,
		keyPrefix: makeKeyPrefix(key),
		handler:   handler,
		quit:      NewDoneChan(),
		quitWatch: NewDoneChan(),
		logger:    stdLogger{},
	}
	l.reset()
	for _, opt := range opts {
		opt(l)
	}
	return l
}

func (l *EtcdSeq) Start() error {
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

func (l *EtcdSeq) Stop() {
	l.quitWatch.Close()
	l.wgWatch.Wait()
	l.quit.Close()
	l.wg.Wait()
}

func (l *EtcdSeq) reset() {
	l.info.Reset()
	l.lease = clientv3.NoLease
}

func (l *EtcdSeq) doKeepAlive() error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-l.quit.Done():
			l.logger.Infof("doKeepAlive quit")
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

func (l *EtcdSeq) keepAliveAsync() error {
	ch, err := l.client.KeepAlive(l.client.Ctx(), l.lease)
	if err != nil {
		return fmt.Errorf("etcd KeepAlive: %w", err)
	}

	l.wg.Add(1)
	go l.runSafe(func() {
		defer l.wg.Done()

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
				l.logger.Infof("keepAliveAsync quit")
				return
			}
		}
	})

	return nil
}

func (l *EtcdSeq) watchAsync() {
	l.wgWatch.Add(1)
	go l.runSafe(func() {
		defer l.wgWatch.Done()

		if err := l.watch(); err != nil {
			l.logger.Errorf("etcd publisher watch: %+v", err)
		}
	})
}

func (l *EtcdSeq) doRegister() (err error) {
	lease, err := l.register()
	if err != nil {
		return fmt.Errorf("etcd register: %w", err)
	}
	l.lease = lease
	l.logger.Debugf("registerred: %s", l.fullKey)
	return l.handleInfoChange()
}

func (l *EtcdSeq) register() (clientv3.LeaseID, error) {
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

func (l *EtcdSeq) doRevoke() {
	err := l.revoke()
	if err != nil {
		l.logger.Errorf("revoke: %+v", err)
		return
	}
	l.reset()
	l.handler.OnChange(l.info)
}

func (l *EtcdSeq) revoke() error {
	_, err := l.client.Revoke(l.client.Ctx(), l.lease)
	if err != nil {
		l.logger.Errorf("etcd Revoke: %+v", err)
	}
	return err
}

func (l *EtcdSeq) load() (info Info, err error) {
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
	return fmt.Sprintf("%s/seat/%d", key, id)
}

func makeKeyPrefix(key string) string {
	return key + "/seat/"
}

func makeLockKey(key string, index int) string {
	return fmt.Sprintf("%s/lk/%d", key, index)
}

func (l *EtcdSeq) watch() error {
	var rch clientv3.WatchChan
	rev := l.info.revision
	if rev == 0 {
		rch = l.client.Watch(clientv3.WithRequireLeader(l.client.Ctx()), l.keyPrefix, clientv3.WithPrefix())
	} else {
		rch = l.client.Watch(clientv3.WithRequireLeader(l.client.Ctx()), l.keyPrefix, clientv3.WithPrefix(), clientv3.WithRev(rev+1))
	}

	for {
		select {
		case <-l.quitWatch.Done():
			l.logger.Infof("watch quit")
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
			l.logger.Debugf("watch event")
			err := l.handleInfoChange()
			if err != nil {
				return fmt.Errorf("etcd handleInfoChange: %w", err)
			}
		}
	}
}

func (l *EtcdSeq) handleInfoChange() error {
	info, err := l.load()
	if err != nil {
		return fmt.Errorf("load: %w", err)
	}
	l.logger.Debugf("loaded: %+v", info)
	if l.info.Index != info.Index || l.info.Count != info.Count {
		l.logger.Debugf("info changed from: %+v to %+v", l.info, info)
		l.handler.OnChange(info)
		l.info = info
	}
	return nil
}

type Unlocker func(ctx context.Context) error

func (l *EtcdSeq) Lock(ctx context.Context, info Info) (Unlocker, error) {
	s, err := concurrency.NewSession(l.client)
	if err != nil {
		return nil, fmt.Errorf("etcd NewSession: %w", err)
	}
	m := concurrency.NewMutex(s, makeLockKey(l.key, info.Index))
	err = m.Lock(ctx)
	if err != nil {
		return nil, fmt.Errorf("etcd Lock: %w", err)
	}
	return m.Unlock, nil
}

func WithLogger(logger Logger) EtcdSeqOption {
	return func(client *EtcdSeq) {
		client.logger = logger
	}
}
