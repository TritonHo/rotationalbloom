package rotationalbloom

import (
	//	"log"

	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis"

	"github.com/tritonho/rotationalbloom/bloom"
)

type impl struct {
	redisClient redis.Cmdable

	// the parameter for rotation
	config Config

	oldBlooms []bloom.BloomFilter
	// we need to remember which index contains the oldest bloom
	oldestIndex int

	// contains the aggregated "oldBlooms", it will speed up the performance of GetAppxCount()
	aggregatedOldBloom bloom.BloomFilter

	// the bloomfilter stored in local machine
	currentBloom bloom.BloomFilter

	// a lock to protect the meta structure of the blooms and locations
	// it protect on the object pointer, NOT the content inside the object
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

	// to minimize the same location upload concurrently(it doesn't affect correctness, but waste the CPU and bandwidth)
	// we use a random seed to affect the uploading sequence of the locations
	// it will be randomized in every rotation
	uploadSeed int
}

func (im *impl) GetAppxCount() float64 {
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
	for _, b := range im.oldBlooms {
		if b.Check(s) {
			return true
		}
	}

	return im.currentBloom.Check(s)
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

	// remarks: we must maintein the metalock lock in the whole process, otherwise in between step 2 and 3 can be race condition
	im.metaLock.RLock()
	defer im.metaLock.RUnlock()

	currentTime := time.Now()

	// step 1: download the redis bloom
	m, k := im.currentBloom.M(), im.currentBloom.K()
	remoteBloom, err := im.downloadFromRedis(currentTime, m, k)
	if err != nil {
		// FIXME: handle the error
		return
	}

	// step 2: merge the remote redis bloom to local current bloom
	im.currentBloom.Merge(remoteBloom)

	// step 3: sync the location to redis
	im.locationLock.RLock()
	candidates, toBeDeleted := im.getUploadCandidates(remoteBloom, im.recentLocations, false)
	im.locationLock.RUnlock()
	if err := im.uploadLocations(currentTime, candidates); err != nil {
		// FIXME: handle the error
	} else {
		toBeDeleted = append(toBeDeleted, candidates...)
	}

	// step 4: remove the uploaded / no longer needed locations
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

// download the centralized bloomfilter from the redis
func (im *impl) downloadFromRedis(t time.Time, m, k int) (bloom.BloomFilter, error) {
	redisKey := im.redisKeyName(t)
	s, err := im.redisClient.Get(redisKey).Result()
	if err != nil {
		return nil, err
	}

	b1 := bloom.NewFromRedis(s, m, k)
	return b1, nil
}

// get the upload candidates from the input
func (im *impl) getUploadCandidates(remoteBloom bloom.BloomFilter, locations map[int]bool, isAll bool) (candidates []int, toBeDeleted []int) {
	candidates = []int{}
	toBeDeleted = []int{}

	for loc, _ := range locations {
		if remoteBloom.TestLocation(loc) {
			toBeDeleted = append(toBeDeleted, loc)
		} else {
			candidates = append(candidates, loc)
		}
		// we want to get the 20% locations "nearest" to the upload seed
		if len(candidates) >= im.config.UploadPerSync*5 && isAll == false {
			break
		}
	}

	if isAll == false && len(candidates) > im.config.UploadPerSync {
		// sorting, according to the seed location
		fn := func(i, j int) bool {
			diff1 := candidates[i] - im.uploadSeed
			diff2 := candidates[j] - im.uploadSeed
			if diff1 < 0 {
				diff1 += remoteBloom.M()
			}
			if diff2 < 0 {
				diff2 += remoteBloom.M()
			}
			return diff1 < diff2
		}
		sort.Slice(candidates, fn)
	}

	return
}

// upload the location to the redis
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
	im.metaLock.Lock()
	m, k := im.currentBloom.M(), im.currentBloom.K()
	im.aggregatedOldBloom = bloom.New(m, k)
	im.metaLock.Unlock()

	im.metaLock.RLock()
	for _, b := range im.oldBlooms {
		if b != nil {
			im.aggregatedOldBloom.Merge(b)
		}
	}
	im.metaLock.RUnlock()
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
	prevCurrentBloom := im.currentBloom
	im.oldBlooms[im.oldestIndex] = im.currentBloom
	im.currentBloom = bloom.New(prevCurrentBloom.M(), prevCurrentBloom.K())

	// step 3: finally, move the index of the rotation
	if im.oldestIndex == len(im.oldBlooms)-1 {
		im.oldestIndex = 0
	} else {
		im.oldestIndex = im.oldestIndex + 1
	}
	// step 4: and then update the seed
	im.uploadSeed = rand.Intn(im.currentBloom.M())
	im.metaLock.Unlock()

	// step 5: flush the locations of previous interval to redis, and then get back the finalized result
	im.flushPrevInterval(oldTime, prevCurrentBloom, prevLocations)

	// step 6: rebuild the agg
	im.rebuildAgg()
}

// flush the locations of previous interval into redis, and then get back the finalized result.
// Remarks: as the operation is not accessing the pointer of the blooms in the impl, only updating the content, thus require no metalock
func (im *impl) flushPrevInterval(oldTime time.Time, prevLocalBloom bloom.BloomFilter, prevLocations map[int]bool) {
	// step 1: download the previous interval remote bloom from redis
	prevRemoteBloom, err1 := im.downloadFromRedis(oldTime, prevLocalBloom.M(), prevLocalBloom.K())
	if err1 != nil {
		// FIXME: handle the error
		return
	}

	// step 2: flush all the previous interval location back to redis
	candidates, _ := im.getUploadCandidates(prevRemoteBloom, prevLocations, true)
	if err := im.uploadLocations(oldTime, candidates); err != nil {
		// FIXME: handle the error
		return
	}

	// step 3: sleep for some seconds to ensure all machine finished the upload of pervious interval's location
	time.Sleep(previousIntervalWaitTime)

	// step 4: download the finalized pervious interval's bloom from redis
	prevRemoteBloom, err2 := im.downloadFromRedis(oldTime, prevLocalBloom.M(), prevLocalBloom.K())
	if err2 != nil {
		// FIXME: handle the error
		return
	}

	// step 5: finally, merge back the result
	prevLocalBloom.Merge(prevRemoteBloom)

}
