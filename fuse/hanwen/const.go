package hanwen

import "math"

const (
	BlockSize     = 4096
	MaximumSpace  = 1024 * 1024 * 1024 * 1024 * 1024 // Set total space to 1PB
	MaximumBlocks = MaximumSpace / BlockSize
	MaximumInodes = math.MaxUint64 // Set maximum inodes to max uint64.
)
