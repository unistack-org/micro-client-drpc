package drpc

import (
	"go.unistack.org/micro/v3/client"
	"go.unistack.org/micro/v3/metadata"
)

type drpcEvent struct {
	payload     interface{}
	topic       string
	contentType string
	opts        client.MessageOptions
}

func newDRPCEvent(topic string, payload interface{}, contentType string, opts ...client.MessageOption) client.Message {
	options := client.NewMessageOptions(opts...)

	if len(options.ContentType) > 0 {
		contentType = options.ContentType
	}

	return &drpcEvent{
		payload:     payload,
		topic:       topic,
		opts:        options,
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

func (d *drpcEvent) Metadata() metadata.Metadata {
	return d.opts.Metadata
}
