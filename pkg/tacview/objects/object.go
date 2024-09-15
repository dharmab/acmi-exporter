package objects

import (
	"iter"
	"maps"
	"sync"
	"time"
)

type Object struct {
	ID         uint32
	Time       time.Duration
	properties map[string]string
	mut        sync.Mutex
}

func NewObject(id uint32) *Object {
	return &Object{
		ID:         id,
		properties: make(map[string]string),
	}
}

func (o *Object) SetProperty(p, v string) {
	o.mut.Lock()
	defer o.mut.Unlock()
	o.properties[p] = v
}

func (o *Object) GetProperty(p string) (string, bool) {
	o.mut.Lock()
	defer o.mut.Unlock()
	val, ok := o.properties[p]
	if !ok {
		return "", false
	}
	return val, true
}

func (o *Object) Properties() iter.Seq2[string, string] {
	o.mut.Lock()
	defer o.mut.Unlock()
	return maps.All(o.properties)
}
