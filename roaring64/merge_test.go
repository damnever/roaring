package roaring64

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMerge(t *testing.T) {
	{
		b1 := NewBitmap()
		b2 := NewBitmap()
		data, err := b2.MarshalBinary()
		require.NoError(t, err)
		err = b1.UnsafeMergeFromBuffer(data)
		require.NoError(t, err)
	}

	for u := uint64(0); u < uint64(64); u += 2 {
		b1 := NewBitmap()
		b2 := NewBitmap()
		for i := uint64(0); i <= u; i++ {
			for _, j := range []uint64{
				0,
				1024,
				8192,
				math.MaxUint8,
				math.MaxUint16,
			} {
				base := math.MaxUint32*i + j
				for k := 4000; k < 4256; k++ {
					b2.Add(base + uint64(k))
				}
				for k := 65536; k < 65536+4000; k++ {
					b1.Add(base + uint64(k))
				}
				for k := 3 * 65536; k < 3*65536+9000; k++ {
					b2.Add(base + uint64(k))
				}
				for k := 4 * 65535; k < 4*65535+7000; k++ {
					b1.Add(base + uint64(k))
				}
				for k := 5 * 65535; k < 5*65535+8000; k++ {
					b2.Add(base + uint64(k))
				}
				for k := 6 * 65535; k < 6*65535+10000; k++ {
					b2.Add(base + uint64(k))
				}
				for k := 8 * 65535; k < 8*65535+1000; k++ {
					b1.Add(base + uint64(k))
				}
				for k := 9 * 65535; k < 9*65535+30000; k++ {
					b2.Add(base + uint64(k))
				}
			}
		}

		expected := Or(b1, b2)

		data1, err := b1.MarshalBinary()
		require.NoError(t, err)
		data2, err := b2.MarshalBinary()
		require.NoError(t, err)

		err = b1.UnsafeMergeFromBuffer(data2)
		require.NoError(t, err)
		err = b2.UnsafeMergeFromBuffer(data1)
		require.NoError(t, err)

		require.True(t, expected.Equals(b1))
		require.True(t, expected.Equals(b2))
	}
}
