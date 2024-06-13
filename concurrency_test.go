package etcdseq_test

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WqyJh/etcdseq"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var indexMap = make(map[int]int)

type schedService struct {
	t           *testing.T
	seq         *etcdseq.EtcdSeq
	ch          chan etcdseq.Info
	ctx         context.Context
	cancel      context.CancelFunc
	workService *workService

	info etcdseq.Info
	wg   sync.WaitGroup
}

func NewSchedService(t *testing.T, etcdCli *clientv3.Client, key string) *schedService {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan etcdseq.Info, 100)
	seq := etcdseq.NewEtcdSeq(etcdCli, key, etcdseq.NewChanHandler(ch))
	workService := NewWorkService(t)
	return &schedService{
		t:           t,
		seq:         seq,
		ctx:         ctx,
		cancel:      cancel,
		ch:          ch,
		workService: workService,
	}
}

func (s *schedService) Start() error {
	err := s.seq.Start()
	assert.NoError(s.t, err)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.run()
	}()
	return nil
}

func (s *schedService) Stop() {
	s.t.Logf("Stop schedService %+v\n", s.info)
	s.cancel()
	s.wg.Wait()
	s.seq.Stop()
}

func (m *schedService) run() {
	for {
		select {
		case <-m.ctx.Done():
			m.workService.Stop()
			return
		case info := <-m.ch:
			m.t.Logf("Received %+v\n", info)
			// drain for the latest (it's an optional optimization)
			for i := 0; i < len(m.ch); i++ {
				info = <-m.ch
				m.t.Logf("Drained %+v\n", info)
			}
			if info.Invalid() {
				m.workService.Stop()
				continue
			}
			if m.info.Equal(info) {
				// remain unchanged (it's an optional optimization)
				continue
			}
			m.workService.Stop()

			unlock, err := m.seq.Lock(context.Background(), info)
			if err != nil {
				// lock failed
				m.t.Logf("Lock %+v failed: %v\n", info, err)
				continue
			}
			m.t.Logf("Locked %+v\n", info)

			m.workService.SetUnlocker(unlock)
			m.workService.Start(info)
			m.info = info
		}
	}
}

type workService struct {
	t      *testing.T
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	started atomic.Bool
	unlock  etcdseq.Unlocker
}

func NewWorkService(t *testing.T) *workService {
	ctx, cancel := context.WithCancel(context.Background())
	return &workService{
		t:      t,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (s *workService) SetUnlocker(unlock etcdseq.Unlocker) {
	s.unlock = unlock
}

func (s *workService) Start(info etcdseq.Info) {
	s.started.Store(true)
	s.wg.Add(1)

	go func() {
		defer func() {
			// this should never race
			indexMap[info.Index]--
			assert.Equal(s.t, 0, indexMap[info.Index], "index %d unexpected", info.Index)

			s.t.Logf("Stopped %+v\n", info)
			s.wg.Done()
			s.started.Store(false)
		}()

		s.t.Logf("Started %+v\n", info)

		val := indexMap[info.Index]
		assert.Equal(s.t, 0, val, "index %d already exists", info.Index)

		// this should never race
		indexMap[info.Index]++
		assert.Equal(s.t, 1, indexMap[info.Index], "index %d inc failed", info.Index)

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(time.Millisecond * 200):
				s.t.Logf("Working tick %+v\n", info)
			}
		}
	}()
}

func (s *workService) Stop() {
	if s.started.Load() {
		s.cancel()
		s.wg.Wait()
	}
	if s.unlock != nil {
		err := s.unlock(context.Background())
		if err != nil {
			s.t.Logf("Unlock failed: %v\n", err)
		} else {
			s.unlock = nil
		}
	}
}

func TestEtcdseqConcurrency(t *testing.T) {

	key := "test_key_6"
	N := 16
	M := 3
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			etcdClient, err := clientv3.New(clientv3.Config{
				Endpoints: []string{"http://124.156.192.247:30004"},
			})
			assert.NoError(t, err)
			defer etcdClient.Close()

			for j := 0; j < M; j++ {
				func(jdx int) {
					s := NewSchedService(t, etcdClient, key)
					err = s.Start()
					assert.NoError(t, err)
					defer s.Stop()

					duration := time.Millisecond * time.Duration(rand.Int31n(1000))
					t.Logf("%d:%d sleeping for %s\n", idx, jdx, duration)
					time.Sleep(duration)
				}(j)
			}
		}(i)
	}

	wg.Wait()
	time.Sleep(time.Second)
}
