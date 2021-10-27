package drpc

import (
	"fmt"
	"strings"

	"go.unistack.org/micro/v3/client"
	"go.unistack.org/micro/v3/codec"
)

type drpcRequest struct {
	request     interface{}
	codec       codec.Codec
	service     string
	method      string
	contentType string
	opts        client.RequestOptions
}

// service Struct.Method /service.Struct/Method
func methodToDRPC(service, method string) string {
	// no method or already grpc method
	if len(method) == 0 || method[0] == '/' {
		return method
	}

	// assume method is Foo.Bar
	mParts := strings.Split(method, ".")
	if len(mParts) != 2 {
		return method
	}

	if len(service) == 0 {
		return fmt.Sprintf("/%s/%s", mParts[0], mParts[1])
	}

	// return /pkg.Foo/Bar
	return fmt.Sprintf("/%s.%s/%s", strings.Title(service), mParts[0], mParts[1])
}

func newDRPCRequest(service, method string, request interface{}, contentType string, reqOpts ...client.RequestOption) client.Request {
	var opts client.RequestOptions
	for _, o := range reqOpts {
		o(&opts)
	}

	// set the content-type specified
	if len(opts.ContentType) > 0 {
		contentType = opts.ContentType
	}

	return &drpcRequest{
		service:     service,
		method:      method,
		request:     request,
		contentType: contentType,
		opts:        opts,
	}
}

func (d *drpcRequest) ContentType() string {
	return d.contentType
}

func (d *drpcRequest) Service() string {
	return d.service
}

func (d *drpcRequest) Method() string {
	return d.method
}

func (d *drpcRequest) Endpoint() string {
	return d.method
}

func (d *drpcRequest) Codec() codec.Codec {
	return d.codec
}

func (d *drpcRequest) Body() interface{} {
	return d.request
}

func (d *drpcRequest) Stream() bool {
	return d.opts.Stream
}
