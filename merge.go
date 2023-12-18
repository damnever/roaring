package roaring

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/RoaringBitmap/roaring/internal"
)

func (rb *Bitmap) UnsafeMergeFromBuffer(data []byte) error {
	stream := internal.ByteBufferPool.Get().(*internal.ByteBuffer)
	stream.Reset(data)
	defer internal.ByteBufferPool.Put(stream)
	return rb.mergeFrom(stream)
}

// MergeFrom could be unsafe, depending on the type of reader and the possible underlying buffer you provided.
func (rb *Bitmap) MergeFrom(reader io.Reader, cookieHeader ...byte) error {
	stream, ok := reader.(internal.ByteInput)
	if !ok {
		byteBuffer := internal.ByteBufferPool.Get().(*internal.ByteInputAdapter)
		byteBuffer.Reset(reader)
		stream = byteBuffer
	}

	err := rb.mergeFrom(stream, cookieHeader...)

	if !ok {
		internal.ByteInputAdapterPool.Put(stream.(*internal.ByteInputAdapter))
	}
	return err
}

func (rb *Bitmap) mergeFrom(stream internal.ByteInput, cookieHeader ...byte) error {
	sr, err := newStreamReader(stream, cookieHeader...)
	if err != nil {
		return err
	}
	willNeedCopyOnWrite := sr.willNeedCopyOnWrite()

	pos1 := 0
	length1 := rb.highlowcontainer.size()
	for (pos1 < length1) && sr.hasNext() {
		s1 := rb.highlowcontainer.getKeyAtIndex(pos1)
		s2 := sr.key()

		if s1 < s2 {
			pos1++
		} else if s1 > s2 {
			c2, err := sr.parseContainerAndAdvance()
			if err != nil {
				return err
			}
			rb.highlowcontainer.insertNewKeyValueCopyOnWrite(pos1, s2, c2, willNeedCopyOnWrite)
			pos1++
			length1++
		} else {
			c2, err := sr.parseContainerAndAdvance()
			if err != nil {
				return err
			}
			rb.highlowcontainer.replaceKeyAndContainerAtIndex(pos1, s1, rb.highlowcontainer.getUnionedWritableContainer(pos1, c2), false)
			pos1++
		}
	}

	if pos1 == length1 {
		for sr.hasNext() {
			key := sr.key()
			c, err := sr.parseContainerAndAdvance()
			if err != nil {
				return err
			}
			rb.highlowcontainer.appendContainer(key, c, willNeedCopyOnWrite)
		}
	}
	return nil
}

type streamReader struct {
	stream internal.ByteInput

	cookie    uint32
	runBitmap []byte
	keycard   []uint16

	nextc container

	next uint32
	size uint32
}

func newStreamReader(stream internal.ByteInput, cookieHeader ...byte) (*streamReader, error) {
	var cookie uint32
	var err error
	if len(cookieHeader) > 0 && len(cookieHeader) != 4 {
		return nil, fmt.Errorf("error in roaringArray.readFrom: could not read initial cookie: incorrect size of cookie header")
	}
	if len(cookieHeader) == 4 {
		cookie = binary.LittleEndian.Uint32(cookieHeader)
	} else {
		cookie, err = stream.ReadUInt32()
		if err != nil {
			return nil, fmt.Errorf("error in roaringArray.readFrom: could not read initial cookie: %s", err)
		}
	}

	var size uint32
	var isRunBitmap []byte

	if cookie&0x0000FFFF == serialCookie {
		size = uint32(cookie>>16 + 1)
		// create is-run-container bitmap
		isRunBitmapSize := (int(size) + 7) / 8
		isRunBitmap, err = stream.Next(isRunBitmapSize)

		if err != nil {
			return nil, fmt.Errorf("malformed bitmap, failed to read is-run bitmap, got: %s", err)
		}
	} else if cookie == serialCookieNoRunContainer {
		size, err = stream.ReadUInt32()
		if err != nil {
			return nil, fmt.Errorf("malformed bitmap, failed to read a bitmap size: %s", err)
		}
	} else {
		return nil, fmt.Errorf("error in roaringArray.readFrom: did not find expected serialCookie in header")
	}

	if size > (1 << 16) {
		return nil, fmt.Errorf("it is logically impossible to have more than (1<<16) containers")
	}

	// descriptive header
	buf, err := stream.Next(2 * 2 * int(size))

	if err != nil {
		return nil, fmt.Errorf("failed to read descriptive header: %s", err)
	}

	keycard := byteSliceAsUint16Slice(buf)

	if isRunBitmap == nil || size >= noOffsetThreshold {
		if err := stream.SkipBytes(int(size) * 4); err != nil {
			return nil, fmt.Errorf("failed to skip bytes: %s", err)
		}
	}

	return &streamReader{
		stream: stream,

		cookie:    cookie,
		runBitmap: isRunBitmap,
		keycard:   keycard,

		next: 0,
		size: size,
	}, nil
}

func (sr *streamReader) hasNext() bool {
	return sr.next < sr.size
}

func (sr *streamReader) willNeedCopyOnWrite() bool {
	return !sr.stream.NextReturnsSafeSlice()
}

func (sr *streamReader) key() uint16 {
	return sr.keycard[2*sr.next]
}

func (sr *streamReader) parseContainerAndAdvance() (container, error) {
	i := sr.next
	card := int(sr.keycard[2*i+1]) + 1

	if sr.runBitmap != nil && sr.runBitmap[i/8]&(1<<(i%8)) != 0 {
		// run container
		nr, err := sr.stream.ReadUInt16()

		if err != nil {
			return nil, fmt.Errorf("failed to read runtime container size: %s", err)
		}

		buf, err := sr.stream.Next(int(nr) * 4)

		if err != nil {
			return nil, fmt.Errorf("failed to read runtime container content: %s", err)
		}

		sr.next++
		return &runContainer16{
			iv: byteSliceAsInterval16Slice(buf),
		}, nil
	} else if card > arrayDefaultMaxSize {
		// bitmap container
		buf, err := sr.stream.Next(arrayDefaultMaxSize * 2)

		if err != nil {
			return nil, fmt.Errorf("failed to read bitmap container: %s", err)
		}

		sr.next++
		return &bitmapContainer{
			cardinality: card,
			bitmap:      byteSliceAsUint64Slice(buf),
		}, nil
	} else {
		// array container
		buf, err := sr.stream.Next(card * 2)

		if err != nil {
			return nil, fmt.Errorf("failed to read array container: %s", err)
		}

		sr.next++
		return &arrayContainer{
			byteSliceAsUint16Slice(buf),
		}, nil
	}
}
