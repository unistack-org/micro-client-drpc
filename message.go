package drpc

import (
	"github.com/unistack-org/micro/v3/client"
)

type drpcEvent struct {
	payload     interface{}
	topic       string
	contentType string
}

func newDRPCEvent(topic string, payload interface{}, contentType string, opts ...client.MessageOption) client.Message {
	var options client.MessageOptions
	for _, o := range opts {
		o(&options)
	}

	if len(options.ContentType) > 0 {
		contentType = options.ContentType
	}

	return &drpcEvent{
		payload:     payload,
		topic:       topic,
		contentType: contentType,
	}
}

func (d *drpcEvent) ContentType() string {
	return d.contentType
}

func (d *drpcEvent) Topic() string {
	return d.topic
}

func (d *drpcEvent) Payload() interface{} {
	return d.payload
}
