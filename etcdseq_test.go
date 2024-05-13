package etcdseq_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/WqyJh/etcdseq"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type testHandler struct {
	t    *testing.T
	name string
	info etcdseq.Info
}

func (h *testHandler) OnChange(info etcdseq.Info) {
	h.t.Helper()
	h.t.Logf("%s info: %+v", h.name, info)
	h.info = info
}

func TestEtcdseq(t *testing.T) {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"http://localhost:2379"},
	})
	assert.NoError(t, err)
	defer etcdClient.Close()

	key := "test_key_1"

	handler := &testHandler{name: "seq1", t: t}
	seq := etcdseq.NewEtcdSeq(etcdClient, key, handler)
	err = seq.Start()
	assert.NoError(t, err)
	assert.Equal(t, 0, handler.info.Index)
	assert.Equal(t, 1, handler.info.Count)

	handler2 := &testHandler{name: "seq2", t: t}
	seq2 := etcdseq.NewEtcdSeq(etcdClient, key, handler2)
	err = seq2.Start()
	assert.NoError(t, err)
	time.Sleep(time.Millisecond)
	assert.Equal(t, 0, handler.info.Index)
	assert.Equal(t, 2, handler.info.Count)
	assert.Equal(t, 1, handler2.info.Index)
	assert.Equal(t, 2, handler2.info.Count)

	seq.Stop()
	time.Sleep(time.Millisecond)
	assert.True(t, handler.info.Invalid())
	assert.Equal(t, 0, handler2.info.Index)
	assert.Equal(t, 1, handler2.info.Count)

	seq2.Stop()
	time.Sleep(time.Millisecond)
	assert.True(t, handler.info.Invalid())
	assert.True(t, handler2.info.Invalid())
}

func TestEtcdseq2(t *testing.T) {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"http://localhost:2379"},
	})
	assert.NoError(t, err)
	defer etcdClient.Close()

	key := "test_key_2"
	seqs := make([]*etcdseq.EtcdSeq, 0)
	handlers := make([]*testHandler, 0)
	N := 20
	for i := 0; i < N; i++ {
		handler := &testHandler{name: fmt.Sprintf("seq-%d", i), t: t}
		seq := etcdseq.NewEtcdSeq(etcdClient, key, handler)
		err = seq.Start()
		assert.NoError(t, err)
		seqs = append(seqs, seq)
		handlers = append(handlers, handler)
		time.Sleep(10 * time.Millisecond)

		for j := 0; j <= i; j++ {
			assert.Equal(t, j, handlers[j].info.Index)
			assert.Equal(t, i+1, handlers[j].info.Count)
		}
	}

	for i := 0; i < N; i++ {
		seqs[i].Stop()
		assert.True(t, handlers[i].info.Invalid())
		time.Sleep(10 * time.Millisecond)

		for j := i + 1; j < N; j++ {
			t.Logf("i: %d, j: %d %+v", i, j, handlers[j].info)
			assert.Equal(t, j-i-1, handlers[j].info.Index)
			assert.Equal(t, N-i-1, handlers[j].info.Count)
		}
	}
}

func TestEtcdClientError(t *testing.T) {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"http://localhost:2379"},
	})
	assert.NoError(t, err)
	etcdClient.Close()

	key := "test_key_3"
	handler := &testHandler{name: "seq1", t: t}
	seq := etcdseq.NewEtcdSeq(etcdClient, key, handler)
	err = seq.Start()
	assert.Error(t, err)
	t.Logf("error: %v", err)
	seq.Stop()
}

type mockLogger struct {
	t      *testing.T
	called bool
}

func (l *mockLogger) Debugf(format string, args ...interface{}) {
	l.t.Helper()
	l.t.Logf("[DEBUG] "+format, args...)
}

func (l *mockLogger) Infof(format string, args ...interface{}) {
	l.t.Helper()
	l.t.Logf("[INFO] "+format, args...)
}

func (l *mockLogger) Errorf(format string, args ...interface{}) {
	l.t.Helper()
	l.t.Logf("[ERROR] "+format, args...)
	l.called = true
}

func TestEtcdClientClose(t *testing.T) {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"http://localhost:2379"},
	})
	assert.NoError(t, err)

	key := "test_key_4"
	logger := mockLogger{t: t}
	handler := &testHandler{name: "seq1", t: t}
	seq := etcdseq.NewEtcdSeq(etcdClient, key, handler, etcdseq.WithLogger(&logger))
	err = seq.Start()
	assert.NoError(t, err)

	etcdClient.Close()
	time.Sleep(time.Second)
	assert.True(t, logger.called)
	seq.Stop()
	time.Sleep(time.Millisecond)

	// cleanup
	etcdClient, err = clientv3.New(clientv3.Config{
		Endpoints: []string{"http://localhost:2379"},
	})
	assert.NoError(t, err)
	etcdClient.Delete(etcdClient.Ctx(), key, clientv3.WithPrefix())
}

func TestEtcdseqChan(t *testing.T) {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"http://localhost:2379"},
	})
	assert.NoError(t, err)
	defer etcdClient.Close()

	key := "test_key_5"

	ch1 := make(chan etcdseq.Info, 100)
	seq := etcdseq.NewEtcdSeq(etcdClient, key, etcdseq.NewChanHandler(ch1))
	err = seq.Start()
	assert.NoError(t, err)
	info := <-ch1
	assert.Equal(t, 0, info.Index)
	assert.Equal(t, 1, info.Count)

	ch2 := make(chan etcdseq.Info, 100)
	seq2 := etcdseq.NewEtcdSeq(etcdClient, key, etcdseq.NewChanHandler(ch2))
	err = seq2.Start()
	assert.NoError(t, err)
	time.Sleep(time.Millisecond)
	info = <-ch1
	assert.Equal(t, 0, info.Index)
	assert.Equal(t, 2, info.Count)
	info = <-ch2
	assert.Equal(t, 1, info.Index)
	assert.Equal(t, 2, info.Count)

	seq.Stop()
	time.Sleep(time.Millisecond)
	info = <-ch1
	assert.True(t, info.Invalid())
	info = <-ch2
	assert.Equal(t, 0, info.Index)
	assert.Equal(t, 1, info.Count)

	seq2.Stop()
	time.Sleep(time.Millisecond)
	info = <-ch2
	assert.True(t, info.Invalid())
}
