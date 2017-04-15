package redsync

import redis "gopkg.in/redis.v5"

// A Pool maintains a pool of Redis connections.
type Pool interface {
	Get() *redis.Client
}
