package drpc

import (
	"github.com/unistack-org/micro/v3/codec"
	"storj.io/drpc"
)

type wrapMicroCodec struct{ codec.Codec }

func (w *wrapMicroCodec) Marshal(v drpc.Message) ([]byte, error) {
	if m, ok := v.(*codec.Frame); ok {
		return m.Data, nil
	}
	return w.Codec.Marshal(v.(interface{}))
}

func (w *wrapMicroCodec) Unmarshal(d []byte, v drpc.Message) error {
	if d == nil || v == nil {
		return nil
	}
	if m, ok := v.(*codec.Frame); ok {
		m.Data = d
		return nil
	}
	return w.Codec.Unmarshal(d, v.(interface{}))
}
