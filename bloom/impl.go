package bloom

import (
	"math"
	"math/big"
	"math/bits"
	"sync"

	"github.com/spaolacci/murmur3"
)

type impl struct {
	// to protect the concurrent access on bitmap
	bitmapLock sync.RWMutex

	bitmap big.Int
	k      int
	m      int
}

func New(m, k int) BloomFilter {
	return _new(m, k, big.Int{})
}

// the string is the bloomfilter stored in redis
func NewFromRedis(s string, m, k int) BloomFilter {
	// be reminded that redis's BITFIELD has reversed bit-ordering
	// while the "math/big".Int is big-endian
	// thus we have to do the manual conversion
	// reference: https://redis.io/commands/bitfield
	temp := make([]byte, len(s), len(s))
	for i, b := range []byte(s) {
		v := bits.Reverse8(uint8(b))
		temp[len(s)-i-1] = byte(v)
	}

	// put the data into the bitmap
	bitmap := big.Int{}
	bitmap.SetBytes(temp)

	return _new(m, k, bitmap)
}

func _new(m, k int, bitmap big.Int) BloomFilter {
	// FIXME: add checking on k and m
	return &impl{
		bitmapLock: sync.RWMutex{},
		bitmap:     bitmap,
		k:          k,
		m:          m,
	}
}

func (im *impl) Merge(g BloomFilter) error {
	if im.k != g.K() || im.m != im.M() {
		return ErrSizeMismatch
	}
	g1, ok := g.(*impl)
	if !ok {
		return ErrImplMismatch
	}
	g1.bitmapLock.RLock()
	im.bitmapLock.Lock()

	im.bitmap.Or(&im.bitmap, &g1.bitmap)

	defer im.bitmapLock.Unlock()
	defer g1.bitmapLock.RUnlock()

	return nil
}
func (im *impl) Clone() BloomFilter {
	return _new(im.m, im.k, im.bitmap)
}

func getLocations(s string, m, k int) []int {
	// use murmur3 hash
	h := murmur3.New64()
	h.Write([]byte(s))

	output := []int{}
	for i := 0; i < k; i++ {
		v := h.Sum64() % uint64(m)
		output = append(output, int(v))

		// append some byte to get another hash in next iteration
		temp := byte(i % 256)
		h.Write([]byte{temp})
	}

	return output
}

// return the locations that is marked in this Add operation
func (im *impl) Add(s string) (locations []int) {
	output := []int{}

	loc := getLocations(s, im.m, im.k)
	im.bitmapLock.Lock()
	defer im.bitmapLock.Unlock()
	for _, loc := range loc {
		if im.bitmap.Bit(loc) == 0 {
			output = append(output, loc)
			im.bitmap.SetBit(&im.bitmap, loc, 1)
		}
	}
	return output
}
func (im *impl) Check(s string) bool {
	loc := getLocations(s, im.m, im.k)

	im.bitmapLock.RLock()
	defer im.bitmapLock.RUnlock()

	for _, loc := range loc {
		if im.bitmap.Bit(loc) == 0 {
			return false
		}
	}

	return true
}

func (im *impl) TestLocation(loc int) bool {
	im.bitmapLock.RLock()
	defer im.bitmapLock.RUnlock()

	return im.bitmap.Bit(loc) == 1
}
func (im *impl) GetAppxCount() float64 {
	im.bitmapLock.RLock()

	bytes := im.bitmap.Bytes()
	defer im.bitmapLock.RUnlock()

	count := 0
	for _, b := range bytes {
		count = count + bits.OnesCount8(b)
	}

	// sentinal to avoid divide-by-zero
	if count == im.m {
		count = im.m - 1
	}

	m := float64(im.m)
	k := float64(im.k)
	c := float64(count)

	// reference: https://en.wikipedia.org/wiki/Bloom_filter#Approximating_the_number_of_items_in_a_Bloom_filter
	return -1 * m / k * math.Log(1-(c/m))
}

func (im *impl) K() int {
	return im.k
}
func (im *impl) M() int {
	return im.m
}
