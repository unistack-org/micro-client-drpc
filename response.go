package drpc

import (
	"io"

	"go.unistack.org/micro/v3/codec"
	"go.unistack.org/micro/v3/metadata"
)

type response struct {
	//conn   *poolConn
	stream io.ReadWriteCloser
	codec  codec.Codec
}

// Read the response
func (r *response) Codec() codec.Codec {
	return r.codec
}

// read the header
func (r *response) Header() metadata.Metadata {
	return nil
	/*
		meta, err := r.stream.Header()
		if err != nil {
			return metadata.New(0)
		}
		md := metadata.New(len(meta))
		for k, v := range meta {
			md.Set(k, strings.Join(v, ","))
		}
		return md
	*/
}

// Read the undecoded response
func (r *response) Read() ([]byte, error) {
	f := &codec.Frame{}
	//if err := r.codec.ReadBody(&wrapStream{r.stream}, f); err != nil {
	if err := r.codec.ReadBody(r.stream, f); err != nil {
		return nil, err
	}
	return f.Data, nil
}
