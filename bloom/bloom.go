/*
	this package provides tailor-made version of bloomfilter library
	Remarks: all operation here is thread safe
*/

package bloom

import (
	"errors"
)

var ErrSizeMismatch = errors.New(`mismatched M and K during Merge() `)
var ErrImplMismatch = errors.New(`mismatched implementation during Merge() `)

type BloomFilter interface {
	// Warning: only the Bloom with same K and M is mergeable.
	// Warning: if two BloomFilter are of different implementation, they may not be mergeable
	// otherwise, error will be raised
	Merge(g BloomFilter) error
	Clone() BloomFilter

	// return the locations that is marked in this Add operation
	Add(s string) (locations []int)
	Check(s string) bool

	TestLocation(loc int) bool
	GetAppxCount() float64

	K() int
	M() int
}
