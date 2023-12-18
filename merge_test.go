package roaring

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMerge(t *testing.T) {
	b1 := NewBitmap()
	b2 := NewBitmap()
	for k := 4000; k < 4256; k++ {
		b2.AddInt(k)
	}
	for k := 65536; k < 65536+4000; k++ {
		b1.AddInt(k)
	}
	for k := 3 * 65536; k < 3*65536+9000; k++ {
		b2.AddInt(k)
	}
	for k := 4 * 65535; k < 4*65535+7000; k++ {
		b1.AddInt(k)
	}
	for k := 5 * 65535; k < 5*65535+8000; k++ {
		b2.AddInt(k)
	}
	for k := 6 * 65535; k < 6*65535+10000; k++ {
		b2.AddInt(k)
	}
	for k := 8 * 65535; k < 8*65535+1000; k++ {
		b1.AddInt(k)
	}
	for k := 9 * 65535; k < 9*65535+30000; k++ {
		b2.AddInt(k)
	}
	expected := Or(b1, b2)

	data2, err := b2.MarshalBinary()
	require.NoError(t, err)
	data1, err := b1.MarshalBinary()
	require.NoError(t, err)

	copyBytes := func(x []byte) []byte {
		y := make([]byte, len(x))
		copy(y, x)
		return y
	}

	b11 := NewBitmap()
	b22 := NewBitmap()
	err = b11.UnsafeMergeFromBuffer(copyBytes(data1))
	require.NoError(t, err)
	err = b22.UnsafeMergeFromBuffer(copyBytes(data2))
	require.NoError(t, err)

	for i := range b11.highlowcontainer.containers {
		require.True(t, b11.highlowcontainer.needsCopyOnWrite(i), i)
	}
	for i := range b22.highlowcontainer.containers {
		require.True(t, b22.highlowcontainer.needsCopyOnWrite(i), i)
	}

	require.True(t, b11.Equals(b1))
	require.True(t, b22.Equals(b22))
	err = b11.UnsafeMergeFromBuffer(data2)
	require.NoError(t, err)
	require.True(t, expected.Equals(b11))

	err = b22.UnsafeMergeFromBuffer(data1)
	require.NoError(t, err)
	require.True(t, expected.Equals(b22))
}
