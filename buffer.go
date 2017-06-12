package main

import (
	"fmt"
	"io"
	"math/rand"

	. "github.com/claudetech/loggo/default"
)

// Buffer buffers the stream and stores chunks
type Buffer struct {
	id     string
	client *Drive
	cache  *Cache
	object *APIObject
	offset int64
	stream io.ReadCloser
}

// NewBuffer creates a new buffer
func NewBuffer(client *Drive, cache *Cache, object *APIObject) (*Buffer, error) {
	id := fmt.Sprintf("%v:%v", object.ObjectID, rand.Int63n(9999))
	return &Buffer{
		id:     id,
		client: client,
		cache:  cache,
		object: object,
	}, nil
}

// Close closes all open stream handlers
func (b *Buffer) Close() error {
	if nil != b.stream {
		if err := b.stream.Close(); nil != err {
			Log.Debugf("%v", err)
			return fmt.Errorf("Could not close stream %v", b.id)
		}
	}
	return nil
}

// Read reads the requested chunk
func (b *Buffer) Read(offset, size int64) ([]byte, error) {
	// disabled preload
	// if uint64(offset+size) < b.object.Size {
	// 	defer func() {
	// 		go b.readBytes(offset+size, size)
	// 	}()
	// }

	return b.readBytes(offset, size)
}

func (b *Buffer) readBytes(offset, size int64) ([]byte, error) {
	id := fmt.Sprintf("%v:%v", b.object.ObjectID, offset)

	chunk, err := b.cache.LoadChunk(id)
	if nil == err {
		Log.Debugf("Found chunk %v in cache", id)
		return chunk.Bytes, nil
	}

	Log.Debugf("Loading chunk %v from API", id)
	bytes, err := b.readFromAPI(offset, size)
	if nil != err {
		return nil, err
	}

	b.cache.StoreChunk(&Chunk{
		ID:       id,
		ObjectID: b.object.ObjectID,
		Offset:   offset,
		Size:     size,
		Bytes:    bytes,
	})

	return bytes, nil
}

func (b *Buffer) readFromAPI(offset, size int64) ([]byte, error) {
	if uint64(offset) > b.object.Size {
		return nil, io.EOF
	}

	if b.shouldReopen(offset, size) {
		if nil != b.stream {
			if err := b.stream.Close(); nil != err {
				Log.Warningf("Could not close old stream handler %v", b.id)
			}
		}

		Log.Debugf("Open new stream handler %v at offset %v", b.id, offset)
		stream, err := b.client.Open(b.object, offset)
		if nil != err {
			Log.Debugf("%v", err)
			return nil, fmt.Errorf("Could not open stream %v", b.id)
		}
		b.stream = stream
		b.offset = offset
	}

	buffer := make([]byte, size)
	n, err := b.stream.Read(buffer)
	if nil != err && io.EOF != err {
		Log.Debugf("%v", err)
		return nil, fmt.Errorf("Could not read bytes at offset %v for stream %v", offset, b.id)
	}
	b.offset += int64(n)

	return buffer, nil
}

func (b *Buffer) shouldReopen(offset, size int64) bool {
	return nil == b.stream || offset != b.offset
}
