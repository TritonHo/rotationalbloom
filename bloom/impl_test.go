package bloom

/*
	remarks: to test the compatibility with redis BITFIELD instruction
	The testcase need real redis for testing

*/
import (
	"math/rand"
	"testing"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
)

// copied from https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go/22892986#22892986
func randString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func TestAddandCheck(t *testing.T) {
	m := 10001
	k := 3
	round := 100
	inputLength := 20
	inputCount := 50

	// given very high m, and low inputCount, the false positive should be close to zero
	for i := 0; i < round; i++ {
		bloom := New(m, k)
		input := []string{}

		// add the input
		for a := 0; a < inputCount; a++ {
			s := randString(inputLength)
			input = append(input, s)
			bloom.Add(s)
		}

		// verify the check
		for _, s := range input {
			assert.True(t, bloom.Check(s))
		}

		// verify some random string
		for a := 0; a < 10; a++ {
			s := randString(inputLength)
			assert.False(t, bloom.Check(s))
		}

		// verift the GetAppxCount
		appx := bloom.GetAppxCount()
		assert.True(t, 48 <= appx && appx <= 52)
	}
}

func TestMerge(t *testing.T) {
	m := 10001
	k := 3

	s1 := `abcd`
	s2 := `1234`
	s3 := `plmqx`

	bloom1 := New(m, k)
	bloom2 := New(m, k)

	bloom1.Add(s1)
	bloom2.Add(s2)

	assert.NoError(t, bloom1.Merge(bloom2))

	assert.True(t, bloom1.Check(s1))
	assert.True(t, bloom1.Check(s2))
	assert.False(t, bloom1.Check(s3))

	assert.False(t, bloom2.Check(s1))
	assert.True(t, bloom2.Check(s2))
	assert.False(t, bloom2.Check(s3))
}

// to ensure the "getLocations" is evenly distributed
func TestGetLocations(t *testing.T) {
	m := 51
	k := 3
	round := 10000
	inputLength := 20

	result := make([]int, m, m)

	for i := 0; i < round; i++ {
		s := randString(inputLength)

		for _, loc := range getLocations(s, m, k) {
			result[loc]++
		}
	}

	chiSquare := float64(0)
	avg := float64(round) * float64(k) / float64(m)

	for _, v := range result {
		diff := float64(v) - avg
		chiSquare = chiSquare + (diff * diff / avg)
	}

	// by some look up table, the chiSquare for (m = 51 should be <= 67.505 for p = 0.05)
	assert.True(t, chiSquare < 67.505, "the chiSquare with 100 degree of freedom")
}

// test the compatibility with redis BITFIELD
func TestNewFromRedis(t *testing.T) {

	// redis on localhost:6379
	redisClient := redis.NewClient(&redis.Options{})

	m := 10001
	k := 3

	s1 := `abcd`
	s2 := `1234`
	s3 := `plmqx`

	bloom1 := New(m, k)
	locations := []int{}
	locations = append(locations, bloom1.Add(s1)...)
	locations = append(locations, bloom1.Add(s2)...)

	// construct the bloomfilter in redis
	redisAgrs := []interface{}{}
	for _, loc := range locations {
		redisAgrs = append(redisAgrs, `set`, `u1`, loc, 1)
	}
	redisKey := `TestNewFromRedis`
	err0 := redisClient.Del(redisKey).Err()
	assert.NoError(t, err0)
	err1 := redisClient.BitField(redisKey, redisAgrs...).Err()
	assert.NoError(t, err1)

	s, err2 := redisClient.Get(redisKey).Result()
	assert.NoError(t, err2)

	bloom2 := NewFromRedis(s, m, k)

	assert.True(t, bloom2.Check(s1))
	assert.True(t, bloom2.Check(s2))
	assert.False(t, bloom2.Check(s3))

}
