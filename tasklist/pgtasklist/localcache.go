package pgtasklist

import "sync"

// localcache 缓存任务id
type localcache struct {
	lock  sync.Mutex
	cache map[int]*pgTask
}

func newLocalcache() *localcache {
	return &localcache{
		cache: make(map[int]*pgTask),
	}
}

func (local *localcache) del(id int) {
	local.lock.Lock()
	defer local.lock.Unlock()

	t := local.cache[id]
	if t != nil {
		close(t.aborted)
		delete(local.cache, id)
	}
}

func (local *localcache) init(ids ...int) {
	local.lock.Lock()
	defer local.lock.Unlock()

	for _, id := range ids {
		local.cache[id] = nil
	}
}

func (local *localcache) set(id int, t *pgTask) {
	local.lock.Lock()
	defer local.lock.Unlock()

	local.cache[id] = t
}

func (local *localcache) getIds() []int {
	local.lock.Lock()
	defer local.lock.Unlock()

	res := make([]int, 0, len(local.cache))
	for id := range local.cache {
		res = append(res, id)
	}
	return res
}
