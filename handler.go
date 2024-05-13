package etcdseq

type ChanHandler struct {
	ch chan<- Info
}

func NewChanHandler(ch chan<- Info) *ChanHandler {
	return &ChanHandler{
		ch: ch,
	}
}

func (h *ChanHandler) OnChange(info Info) {
	h.ch <- info
}
