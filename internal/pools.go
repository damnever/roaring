package internal

import (
	"sync"
)

var (
	// ByteInputAdapterPool shared pool
	ByteInputAdapterPool = NewTypedPool(func() *ByteInputAdapter { return &ByteInputAdapter{} })

	// ByteBufferPool shared pool
	ByteBufferPool = NewTypedPool(func() *ByteBuffer { return &ByteBuffer{} })
)

type TypedPool[T any] struct {
	factory func() T
	pool    sync.Pool
}

func NewTypedPool[T any](factory func() T) *TypedPool[T] {
	return &TypedPool[T]{
		factory: factory,
		pool:    sync.Pool{},
	}
}

func (p *TypedPool[T]) Get() T {
	v, ok := p.pool.Get().(T)
	if !ok {
		v = p.factory()
	}
	return v
}

func (p *TypedPool[T]) GetNoCreate() T {
	v, _ := p.pool.Get().(T)
	return v
}

func (p *TypedPool[T]) Put(v T) {
	p.pool.Put(v)
}
