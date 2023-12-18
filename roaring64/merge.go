package roaring64

import (
	"encoding/binary"
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/internal"
)

func (rb *Bitmap) UnsafeMergeFromBuffer(data []byte) error {
	stream := internal.NewByteBuffer(data)
	sr, err := newStreamReader(stream)
	if err != nil {
		return err
	}

	pos1 := 0
	length1 := rb.highlowcontainer.size()
	for (pos1 < length1) && sr.hasNext() {
		s1 := rb.highlowcontainer.getKeyAtIndex(pos1)
		s2 := sr.key()

		if s1 < s2 {
			pos1++
		} else if s1 > s2 {
			c2, err := sr.parseBitmapAndAdvance(nil)
			if err != nil {
				return err
			}
			rb.highlowcontainer.insertNewKeyValueAt(pos1, s2, c2)
			pos1++
			length1++
		} else {
			_, err := sr.parseBitmapAndAdvance(rb.highlowcontainer.getWritableContainerAtIndex(pos1))
			if err != nil {
				return err
			}
			pos1++
		}
	}
	if pos1 == length1 {
		for sr.hasNext() {
			key := sr.key()
			c, err := sr.parseBitmapAndAdvance(nil)
			if err != nil {
				return err
			}
			rb.highlowcontainer.appendContainer(key, c, false)
		}
	}
	return nil
}

type streamReader struct {
	bitmap32     *roaring.Bitmap
	bitmap32Used bool

	stream *internal.ByteBuffer

	curkey uint32

	next uint64
	size uint64
}

func newStreamReader(stream *internal.ByteBuffer) (*streamReader, error) {
	cookieBuf, err := stream.Next(4)
	if err != nil {
		return nil, err
	}
	fileMagic := int(binary.LittleEndian.Uint16(cookieBuf[0:2]))
	if fileMagic == serialCookieNoRunContainer || fileMagic == serialCookie {
		bm32 := roaring.NewBitmap()
		_, err := bm32.ReadFrom(stream, cookieBuf...)
		if err != nil {
			return nil, err
		}
		return &streamReader{bitmap32: bm32}, nil
	}

	var sizeBuf [8]byte // Avoid changing the original byte slice.
	sizeBuf2, err := stream.Next(4)
	if err != nil {
		return nil, fmt.Errorf("could not read number of containers: %w", err)
	}
	copy(sizeBuf[:], cookieBuf)
	copy(sizeBuf[4:], sizeBuf2)
	size := binary.LittleEndian.Uint64(sizeBuf[:])

	sr := &streamReader{stream: stream, next: 0, size: size}
	if sr.hasNext() {
		if err := sr.parseKey(); err != nil {
			return nil, err
		}
	}
	return sr, nil
}

func (sr *streamReader) hasNext() bool {
	return (sr.bitmap32 != nil && !sr.bitmap32Used) || sr.next < sr.size
}

func (sr *streamReader) key() uint32 {
	if sr.bitmap32 != nil {
		return 0
	}
	return sr.curkey
}

func (sr *streamReader) parseKey() error {
	if sr.bitmap32 != nil {
		return nil
	}

	keyBuf, err := sr.stream.Next(4)
	if err != nil {
		return fmt.Errorf("could not read key %d/%d: %w", sr.next, sr.size, err)
	}
	sr.curkey = binary.LittleEndian.Uint32(keyBuf)
	return nil
}

func (sr *streamReader) parseBitmapAndAdvance(bitmap *roaring.Bitmap) (*roaring.Bitmap, error) {
	if sr.bitmap32 != nil {
		sr.bitmap32Used = true
		if bitmap != nil {
			bitmap.Or(sr.bitmap32)
			return bitmap, nil
		}
		return sr.bitmap32, nil
	}
	if bitmap == nil {
		bitmap = roaring.NewBitmap()
	}

	err := bitmap.MergeFrom(sr.stream)
	if err != nil {
		return nil, fmt.Errorf("could not deserialize bitmap for key %d/%d: %w", sr.next, sr.size, err)
	}

	sr.next++
	if sr.hasNext() {
		if err := sr.parseKey(); err != nil {
			return nil, err
		}
	}
	return bitmap, nil
}
