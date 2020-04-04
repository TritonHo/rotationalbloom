package rotationalbloom

// design requirement: all operation should be thread safe
type bloomfilter struct {
	// FIXME: implement it
}

func NewBloom(m, k int) *bloomfilter {
	// FIXME: implement it
	return nil
}

func NewBloomFromString(s string, m, k int) *bloomfilter {
	// FIXME: implement it
	return nil
}

// merge the g to itself
func (bk *bloomfilter) Merge(g *bloomfilter) error {
	// FIXME: implement it
	return nil
}

func (bk *bloomfilter) Clone() *bloomfilter {
	// FIXME: implement it
	return nil
}

// returning the position on newly marked position on the bloomfilter data structure
func (bk *bloomfilter) Add(s string) []int {
	// FIXME: implement it
	return nil
}

func (bk *bloomfilter) Check(s string) bool {
	// FIXME: implement it
	return true
}

// add locations to the bloomfilter
func (bk *bloomfilter) AddLocations(loc []int) {
	// FIXME: implement it
}

// test if a location x is already marked
func (bk *bloomfilter) TestLocation(x int) bool {
	// FIXME: implement it
	return false
}

func (bk *bloomfilter) GetAppxCount() int {
	// FIXME: implement it
	return 0
}
