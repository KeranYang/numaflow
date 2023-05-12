package util

type Rater int64

const (
	ISB           Rater = 0
	SYNC_METRICS  Rater = 1
	ASYNC_METRICS Rater = 2
)
