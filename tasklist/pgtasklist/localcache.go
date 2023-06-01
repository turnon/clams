package pgtasklist

import "sync"

// localcache 缓存任务id
type localcache struct {
	lock  sync.Mutex
	cache map[int]struct{}
}

func newLocalcache() *localcache {
	return &localcache{
		cache: make(map[int]struct{}),
	}
}

func (local *localcache) del(ids ...int) {
	local.lock.Lock()
	defer local.lock.Unlock()

	for _, id := range ids {
		delete(local.cache, id)
	}
}

func (local *localcache) set(ids ...int) {
	local.lock.Lock()
	defer local.lock.Unlock()

	for _, id := range ids {
		local.cache[id] = struct{}{}
	}
}

func (local *localcache) get() []int {
	local.lock.Lock()
	defer local.lock.Unlock()

	res := make([]int, 0, len(local.cache))
	for id := range local.cache {
		res = append(res, id)
	}
	return res
}
