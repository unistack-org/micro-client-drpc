//go:build ignore

package drpc

import (
	"context"
	"io"
	"sync"

	"github.com/unistack-org/micro/v3/client"
)

// Implements the streamer interface
type drpcStream struct {
	// grpc.ClientStream
	context  context.Context
	err      error
	request  client.Request
	response client.Response
	close    func(err error)
	// conn     *poolConn
	sync.RWMutex
	closed bool
}

func (d *drpcStream) Context() context.Context {
	return d.context
}

func (d *drpcStream) Request() client.Request {
	return d.request
}

func (d *drpcStream) Response() client.Response {
	return d.response
}

func (d *drpcStream) Send(msg interface{}) error {
	if err := d.ClientStream.SendMsg(msg); err != nil {
		d.setError(err)
		return err
	}
	return nil
}

func (d *drpcStream) Recv(msg interface{}) (err error) {
	defer d.setError(err)

	if err = d.ClientStream.RecvMsg(msg); err != nil {
		// #202 - inconsistent gRPC stream behavior
		// the only way to tell if the stream is done is when we get a EOF on the Recv
		// here we should close the underlying gRPC ClientConn
		closeErr := d.Close()
		if err == io.EOF && closeErr != nil {
			err = closeErr
		}

		return err
	}

	return
}

func (d *drpcStream) Error() error {
	d.RLock()
	defer d.RUnlock()
	return d.err
}

func (d *drpcStream) setError(e error) {
	d.Lock()
	d.err = e
	d.Unlock()
}

// Close the gRPC send stream
// #202 - inconsistent gRPC stream behavior
// The underlying gRPC stream should not be closed here since the
// stream should still be able to receive after this function call
// TODO: should the conn be closed in another way?
func (d *drpcStream) Close() error {
	d.Lock()
	defer d.Unlock()

	if d.closed {
		return nil
	}

	// close the connection
	d.closed = true
	d.close(d.err)
	return d.ClientStream.CloseSend()
}
