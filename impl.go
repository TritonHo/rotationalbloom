package rotationalbloom

import (
	//	"log"

	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

type impl struct {
	redisClient redis.Cmdable

	// the parameter for the bloomfilter data structure
	m int
	k int

	// the parameter for rotation
	config Config

	oldBlooms []*bloomfilter
	// we need to remember which index contains the oldest bloom
	oldestIndex int

	// contains the aggregated "oldBlooms", it will speed up the performance of GetAppxCount()
	aggregatedOldBloom *bloomfilter

	currentBloom *bloomfilter

	// a lock to protect when the blooms is being updated
	metaLock sync.RWMutex

	// the lock to protect against concurrent update on the locations
	locationLock sync.RWMutex

	// same location marked by two application server concurrently.
	// although it will not raise correctness issue, it will waste the bandwidth and the redis resource
	// thus, every application server will have a initially randomized location, and "sweep" the recentLocations to redis
	sweepLocation int

	// set of location that recently added but not yet added to the redis
	recentLocations map[int]bool

	addBuffer chan string
}

func (im *impl) GetAppxCount() int {
	// correct the information into local variables
	im.metaLock.RLock()
	b := im.currentBloom.Clone()
	b.Merge(im.aggregatedOldBloom)
	im.metaLock.RUnlock()

	// use the local variables to compute the result
	return b.GetAppxCount()
}

func (im *impl) Check(s string) bool {
	im.metaLock.RLock()
	defer im.metaLock.RUnlock()

	return im.currentBloom.Check(s) || im.aggregatedOldBloom.Check(s)
}

// add s to the bloom
func (im *impl) Add(s string) {
	im.addBuffer <- s
}

func (im *impl) addWorkerThread() {
	for s := range im.addBuffer {
		im.metaLock.RLock()
		newLocations := im.currentBloom.Add(s)

		if len(newLocations) > 0 {
			im.locationLock.Lock()
			for _, loc := range newLocations {
				im.recentLocations[loc] = true
			}
			im.locationLock.Unlock()
		}
		im.metaLock.RUnlock()
	}
}
func (im *impl) syncWithRedis() {
	im.metaLock.RLock()
	defer im.metaLock.RUnlock()

	currentTime := time.Now()

	// step 1: merge the redis bloom to local current bloom
	im.downloadFromRedis(currentTime, im.currentBloom)

	// step 2: sync the location to redis
	im.locationLock.RLock()
	candidates, toBeDeleted := im.getUploadCandidates(im.currentBloom, im.recentLocations, false)
	im.locationLock.RUnlock()
	if err := im.uploadLocations(currentTime, candidates); err != nil {
		// FIXME: handle the error
	} else {
		toBeDeleted = append(toBeDeleted, candidates...)
	}

	// step 3: remove the uploaded / no longer needed locations
	if len(toBeDeleted) > 0 {
		im.locationLock.Lock()
		for _, loc := range toBeDeleted {
			delete(im.recentLocations, loc)
		}
		im.locationLock.Unlock()
	}
}
func (im *impl) redisKeyName(t time.Time) string {
	ts := t.Unix()
	interval := int64(im.config.Interval.Seconds())

	ts = ts - (ts % interval)

	return im.config.KeyPrefix + strconv.FormatInt(ts, 10)
}

// download the data from the redis to target bloom
func (im *impl) downloadFromRedis(t time.Time, b *bloomfilter) {
	// FIXME: implement it
}

// upload the location to the redis

// get the upload candidates from the input
func (im *impl) getUploadCandidates(b *bloomfilter, locations map[int]bool, isAll bool) (candidates []int, toBeDeleted []int) {
	candidates = []int{}
	toBeDeleted = []int{}

	// FIXME: implement the non-all version
	candidateLocations := []int{}
	for loc, _ := range locations {
		if b.TestLocation(loc) {
			toBeDeleted = append(toBeDeleted, loc)
		} else {
			candidateLocations = append(candidateLocations, loc)
		}
	}
	return
}
func (im *impl) uploadLocations(t time.Time, candidates []int) error {
	// sentinal
	if len(candidates) == 0 {
		return nil
	}

	redisAgrs := []interface{}{}
	for _, loc := range candidates {
		redisAgrs = append(redisAgrs, `set`, `u1`, loc, 1)
	}

	redisKey := im.redisKeyName(t)
	return im.redisClient.BitField(redisKey, redisAgrs...).Err()
}
func (im *impl) rebuildAgg() {
	im.aggregatedOldBloom = NewBloom(im.m, im.k)
	for _, b := range im.oldBlooms {
		if b != nil {
			im.aggregatedOldBloom.Merge(b)
		}
	}
}

func (im *impl) rotation(currentTime time.Time) {
	// get back the ts of pervious interval
	oldTime := currentTime.Add(-1 * im.config.Interval)

	im.metaLock.Lock()

	// step 1: swap the new locations
	im.locationLock.Lock()
	prevLocations := im.recentLocations
	im.recentLocations = map[int]bool{}
	im.locationLock.Unlock()

	// step 2: swap the current bloom
	im.oldBlooms[im.oldestIndex] = im.currentBloom
	im.currentBloom = NewBloom(im.m, im.k)

	im.metaLock.Unlock()

	// step 3: sync back all the location back to redis
	im.metaLock.RLock()
	candidates, _ := im.getUploadCandidates(im.oldBlooms[im.oldestIndex], prevLocations, true)
	im.metaLock.RUnlock()
	im.uploadLocations(oldTime, candidates)

	// step 4: sleep for 5 seconds to ensure all mechanism finished the upload of pervious interval's location
	time.Sleep(previousIntervalWaitTime)

	// FIXME: there is race condition here
	// step 5: download the finalized pervious interval's bloom from redis
	im.downloadFromRedis(oldTime, im.oldBlooms[im.oldestIndex])

	// step 6: rebuild the agg
	im.metaLock.Lock()
	im.rebuildAgg()
	im.metaLock.Unlock()

	// step 7: finally, move the index of the rotation
	if im.oldestIndex == len(im.oldBlooms)-1 {
		im.oldestIndex = 0
	} else {
		im.oldestIndex = im.oldestIndex + 1
	}
}
