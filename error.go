package redsync

import "errors"

// ErrFailed is the error that acquirement failed
var ErrFailed = errors.New("redsync: failed to acquire lock")
