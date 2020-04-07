package rotationalbloom

import (
	"time"
)

// when we enter into a new interval, we need to wait for all server to upload their marked location.
// After all application servers uploading completed, we will download the finalized bloom into local memory.
// Remarks: we need to consider the processing time of redis, and also the time discrepancy between application servers.
var previousIntervalWaitTime = time.Second * 10

/*
	Summary:
		FIXME
	Reference:
		FIXME
*/

type Config struct {
	KeyPrefix string

	// Interval MUST be measured in second.
	// the smallest interval should be 10 sec.
	// suggestion: 60 sec.
	Interval time.Duration
	// the Interval * IntervalNum = the period you want to measure for the unique items
	IntervalNum int

	// FIXME: the interval between the synchronization with redis
	SyncInterval time.Duration

	// the number of location uploaded per sync
	UploadPerSync int
}

type RotationalBloom interface {
	SetIntervalNum(n int)

	// return the appx number of unique string
	GetAppxCount() float64

	Check(s string) bool

	// add s to the bloom
	Add(s string)
}
