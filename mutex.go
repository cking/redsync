package redsync

import (
	"crypto/rand"
	"encoding/base64"
	"sync"
	"time"

	"gopkg.in/redis.v5"
)

// A Mutex is a distributed mutual exclusion lock.
type Mutex struct {
	name   string
	expiry time.Duration

	tries int
	delay time.Duration

	factor float64

	quorum int

	value string
	until time.Time

	nodem sync.Mutex

	pools []*redis.Client
}

// Lock locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (m *Mutex) Lock() error {
	m.nodem.Lock()
	defer m.nodem.Unlock()

	value, err := m.genValue()
	if err != nil {
		return err
	}

	for i := 0; i < m.tries; i++ {
		if i != 0 {
			time.Sleep(m.delay)
		}

		start := time.Now()

		n := 0
		for _, pool := range m.pools {
			ok := m.acquire(pool, value)
			if ok {
				n++
			}
		}

		until := time.Now().Add(m.expiry - time.Now().Sub(start) - time.Duration(int64(float64(m.expiry)*m.factor)) + 2*time.Millisecond)
		if n >= m.quorum && time.Now().Before(until) {
			m.value = value
			m.until = until
			return nil
		}
		for _, pool := range m.pools {
			m.release(pool, value)
		}
	}

	return ErrFailed
}

// Unlock unlocks m and returns the status of unlock. It is a run-time error if m is not locked on entry to Unlock.
func (m *Mutex) Unlock() bool {
	m.nodem.Lock()
	defer m.nodem.Unlock()

	n := 0
	for _, pool := range m.pools {
		ok := m.release(pool, m.value)
		if ok {
			n++
		}
	}
	return n >= m.quorum
}

// Extend resets the mutex's expiry and returns the status of expiry extension. It is a run-time error if m is not locked on entry to Extend.
func (m *Mutex) Extend() bool {
	m.nodem.Lock()
	defer m.nodem.Unlock()

	n := 0
	for _, pool := range m.pools {
		ok := m.touch(pool, m.value, int(m.expiry/time.Millisecond))
		if ok {
			n++
		}
	}
	return n >= m.quorum
}

func (m *Mutex) genValue() (string, error) {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func (m *Mutex) acquire(conn *redis.Client, value string) bool {
	reply, err := conn.SetNX(m.name, value, m.expiry).Result()
	return err == nil && reply
}

var deleteScript = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	else
		return 0
	end
`)

func (m *Mutex) release(conn *redis.Client, value string) bool {
	status, err := deleteScript.Run(conn, []string{m.name}, value).Result()
	return err == nil && status.(string) != "0"
}

var touchScript = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("SET", KEYS[1], ARGV[1], "XX", "PX", ARGV[2])
	else
		return "ERR"
	end
`)

func (m *Mutex) touch(conn *redis.Client, value string, expiry int) bool {
	rawStatus, err := touchScript.Run(conn, []string{m.name}, value, expiry).Result()
	status := rawStatus.(string)
	return err == nil && status != "ERR"
}
