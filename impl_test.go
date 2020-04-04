package rotationalbloom

// FIXME: we need a real redis for the testing of the lua script. Is there any better idea?
import (
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis"
	//	"github.com/stretchr/testify/assert"
)

// func (im *impl) syncLocations(t time.Time, b *bloomfilter, locations map[int]bool, isAll bool) {

func TestSyncLocations(t *testing.T) {

	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	im := &impl{
		redisClient: redisClient,
		config: Config{
			KeyPrefix:   `keyPrefix-`,
			Interval:    time.Second * 60,
			IntervalNum: 10,
		},
		locationLock: sync.RWMutex{},
	}
	now := time.Now()
	b := &bloomfilter{}
	locations := map[int]bool{
		1: true,
		3: true,
		5: true,
		8: true,
		9: true,
	}
	im.syncLocations(now, b, locations, true)
}
