# etcdseq

Etcdseq is an lightweight library to support sequence nodes via etcd.

Sequence nodes can be achieved by other ways, such as:

- Zookeeper sequence nodes
- K8S StatefulSet

However, if you're using etcd rather than zookeeper and using K8S Deployment
rather than StatefulSet, etcdseq can help.

What's more, etcdseq provides index/count pair as node info, which is useful
when you need the number of total nodes. Zookeeper and K8S only provide index.


## Design

Assume you have 3 nodes, every nodes would be assigned with an index/count pair.

- node1: index=0 count=3
- node2: index=1 count=3
- node3: index=2 count=3

When new nodes added, count would change.

- node1: index=0 count=4
- node2: index=1 count=4
- node3: index=2 count=4
- node4: index=3 count=4

When nodes removed, indexes of the later nodes would change, and count would change too.

- node1: index=0 count=3
- node3: index=1 count=3
- node4: index=2 count=3

## Usage

Simply use an `ChanHandler` to recieve the node change event sequentially.

```go
    // first create an etcd connection
    etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"http://localhost:2379"},
	})

    // create an channel to receive events
    ch := make(chan etcdseq.Info, 100)

    // key is the identifier of the nodes group
    key := "service-group"

    // create an EtcdSeq
    seq := etcdseq.NewEtcdSeq(etcdClient, key, etcdseq.NewChanHandler(ch))

    // start EtcdSeq service
	err = seq.Start()

    // make sure you stop EtcdSeq after you no longer need it.
    seq.Stop()
```

Use the following structure of code to handle the node change event. The key point is `startService(info)` which means you start your own service and allocate the 
resources according to the index/count pair. And remember using `stopService` to
release the resources when node changed. If `info.Invalid()`, then there's no node alive anymore, just stop your service.

```go
func run(ctx context.Context) {
    var oldInfo etcdseq.Info
	for {
		select {
		case <-ctx.Done():
			stopService()
			return
		case info := <-ch:
			// drain for the latest (it's an optional optimization)
			for i := 0; i < len(ch); i++ {
				info = <-ch
			}
			if info.Invalid() {
				stopService()
				continue
			}
			if oldInfo.Equal(info) {
                // remain unchanged (it's an optional optimization)
				continue
			}
            stopService()
            startService(info)
            oldInfo = info
		}
	}
}
```
