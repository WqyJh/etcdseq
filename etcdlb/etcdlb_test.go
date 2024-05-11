package etcdlb_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/WqyJh/etcdlb/etcdlb"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type testHandler struct {
	t    *testing.T
	name string
	info etcdlb.Info
}

func (h *testHandler) OnChange(info etcdlb.Info) {
	h.t.Helper()
	h.t.Logf("%s info: %+v", h.name, info)
	h.info = info
}

func (h *testHandler) OnRevoke() {
	h.t.Helper()
	h.t.Logf("%s revoke", h.name)
	h.info.Reset()
}

func TestEtcdlb(t *testing.T) {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"http://localhost:2379"},
	})
	assert.NoError(t, err)
	defer etcdClient.Close()

	key := "test_key"

	handler := &testHandler{name: "lb1", t: t}
	lb := etcdlb.NewEtcdLB(etcdClient, key, handler)
	err = lb.Start()
	assert.NoError(t, err)
	assert.Equal(t, 0, handler.info.Index)
	assert.Equal(t, 1, handler.info.Count)

	handler2 := &testHandler{name: "lb2", t: t}
	lb2 := etcdlb.NewEtcdLB(etcdClient, key, handler2)
	err = lb2.Start()
	assert.NoError(t, err)
	time.Sleep(time.Millisecond)
	assert.Equal(t, 0, handler.info.Index)
	assert.Equal(t, 2, handler.info.Count)
	assert.Equal(t, 1, handler2.info.Index)
	assert.Equal(t, 2, handler2.info.Count)

	lb.Stop()
	time.Sleep(time.Millisecond)
	assert.True(t, handler.info.Invalid())
	assert.Equal(t, 0, handler2.info.Index)
	assert.Equal(t, 1, handler2.info.Count)

	lb2.Stop()
	time.Sleep(time.Millisecond)
	assert.True(t, handler.info.Invalid())
	assert.True(t, handler2.info.Invalid())
}

func TestEtcdlb2(t *testing.T) {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"http://localhost:2379"},
	})
	assert.NoError(t, err)
	defer etcdClient.Close()

	key := "test_key"
	lbs := make([]*etcdlb.EtcdLB, 0)
	handlers := make([]*testHandler, 0)
	N := 20
	for i := 0; i < N; i++ {
		handler := &testHandler{name: fmt.Sprintf("lb-%d", i), t: t}
		lb := etcdlb.NewEtcdLB(etcdClient, key, handler)
		err = lb.Start()
		assert.NoError(t, err)
		lbs = append(lbs, lb)
		handlers = append(handlers, handler)
		time.Sleep(time.Millisecond)

		for j := 0; j <= i; j++ {
			assert.Equal(t, j, handlers[j].info.Index)
			assert.Equal(t, i+1, handlers[j].info.Count)
		}
	}

	for i := 0; i < N; i++ {
		lbs[i].Stop()
		assert.True(t, handlers[i].info.Invalid())
		time.Sleep(time.Millisecond)

		for j := i + 1; j < N; j++ {
			t.Logf("i: %d, j: %d %+v", i, j, handlers[j].info)
			assert.Equal(t, j-i-1, handlers[j].info.Index)
			assert.Equal(t, N-i-1, handlers[j].info.Count)
		}
	}
}
