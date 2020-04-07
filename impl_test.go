package rotationalbloom

// FIXME: we need a real redis for the testing of the lua script. Is there any better idea?
import (
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"

	"github.com/tritonho/rotationalbloom/bloom"
)

func TestUploadAndDownload(t *testing.T) {

	redisClient := redis.NewClient(&redis.Options{})

	im := &impl{
		redisClient: redisClient,
		config: Config{
			KeyPrefix:   `keyPrefix-`,
			Interval:    time.Second * 60,
			IntervalNum: 10,
		},
		locationLock: sync.RWMutex{},
	}
	currentTime := time.Now()

	m, k := 1001, 1
	b1 := bloom.New(m, k)
	locations := []int{}

	locations = append(locations, b1.Add(`it-is-string-1`)...)
	locations = append(locations, b1.Add(`it-is-string-2`)...)

	err0 := im.uploadLocations(currentTime, locations)
	assert.NoError(t, err0)

	b2 := bloom.New(m, k)

	b2, err1 := im.downloadFromRedis(currentTime, m, k)
	assert.NoError(t, err1)

	// the b2 should have the content from b1
	assert.True(t, b2.Check(`it-is-string-1`))
	assert.True(t, b2.Check(`it-is-string-2`))

	// test some unrelated content to b2
	assert.False(t, b2.Check(`abc`))
	assert.False(t, b2.Check(`def`))
	assert.False(t, b2.Check(`12345678`))
	assert.False(t, b2.Check(`I hate writing testcase`))
}

/*
func TestGetUploadCandidates(t *testing.T) {
	im := &impl{
		redisClient: redisClient,
		config: Config{
			UploadPerSync: 10,
		},
		locationLock: sync.RWMutex{},
	}
	m, k := 10000, 1

	rounds := 1

	for i := 0; i < rounds; i++ {
		b.New(m, k)
		locations := []int{}
		// populate ~10% locations
		for j := 0; j < m/10; j++ {
			s := bloom.RandString(30)
			locations = append(locations, b.Add(s)...)
		}
		im.getUploadCandidates
	}

	// getUploadCandidates(b bloom.BloomFilter, locations map[int]bool, isAll bool) (candidates []int, toBeDeleted []int) {
	im.getUploadCandidates(b)
}
*/
