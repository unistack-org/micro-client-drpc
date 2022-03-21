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
func methodToDRPC(_ string, method string) string {
	// no method or already drpc method
	if len(method) == 0 || method[0] == '/' {
		return method
	}

	idx := strings.LastIndex(method, ".")
	drpcService := method[:idx]
	drpcMethod := method[idx+1:]
	return fmt.Sprintf("/%s/%s", drpcService, drpcMethod)
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
