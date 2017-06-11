package main

import (
	"fmt"
	"io"

	. "github.com/claudetech/loggo/default"
)

// Buffer buffers the stream and stores chunks
type Buffer struct {
	client   *Drive
	object   *APIObject
	position int64
	stream   io.ReadCloser
}

// NewBuffer creates a new buffer
func NewBuffer(client *Drive, object *APIObject) (*Buffer, error) {
	return &Buffer{
		client: client,
		object: object,
	}, nil
}

// Close closes all open stream handlers
func (b *Buffer) Close() error {
	return nil
}

// Read reads the requested chunk
func (b *Buffer) Read(offset int64, size int64) ([]byte, error) {
	if b.shouldReopen(offset) {
		stream, err := b.client.Open(b.object, offset)
		if nil != err {
			Log.Debugf("%v", err)
			return nil, fmt.Errorf("Could not open stream for object %v (%v)", b.object.ObjectID, b.object.Name)
		}
		b.stream = stream
	}

	// TODO: read stuff here

	return nil, nil
}

func (b *Buffer) shouldReopen(offset int64) bool {
	return nil == b.stream
}
