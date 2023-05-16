package server

import (
	"container/list"
	"sync"
)

type UniqueStringList struct {
	l    *list.List
	m    map[string]*list.Element
	lock *sync.RWMutex
}

func NewUniqueStringList() *UniqueStringList {
	return &UniqueStringList{
		l:    list.New(),
		m:    make(map[string]*list.Element),
		lock: new(sync.RWMutex),
	}
}

// PushBack adds a value to the back of the list, if the value doesn't exist in the list.
func (l *UniqueStringList) PushBack(value string) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if _, ok := l.m[value]; !ok {
		l.m[value] = l.l.PushBack(value)
	}
}

// MoveToBack moves the element to the back of the list, if the value exists in the list.
func (l *UniqueStringList) MoveToBack(value string) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if e, ok := l.m[value]; ok {
		l.l.MoveToBack(e)
	}
}

// Front returns the first string of list l or empty string if the list is empty.
func (l *UniqueStringList) Front() string {
	l.lock.RLock()
	defer l.lock.RUnlock()
	if l.Len() == 0 {
		return ""
	}
	return l.l.Front().Value.(string)
}

// Len returns the number of elements of list l.
func (l *UniqueStringList) Len() int {
	l.lock.RLock()
	defer l.lock.RUnlock()
	return l.l.Len()
}

// Contains returns true if the value exists in the list.
func (l *UniqueStringList) Contains(value string) bool {
	l.lock.RLock()
	defer l.lock.RUnlock()
	_, ok := l.m[value]
	return ok
}

// Remove removes the element from the list, if the value exists in the list.
func (l *UniqueStringList) Remove(value string) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if e, ok := l.m[value]; ok {
		l.l.Remove(e)
		delete(l.m, value)
	}
}

// ToString returns a comma separated string of the list values.
func (l *UniqueStringList) ToString() string {
	l.lock.RLock()
	defer l.lock.RUnlock()
	var s string
	for e := l.l.Front(); e != nil; e = e.Next() {
		s += e.Value.(string) + ","
	}
	return s
}
