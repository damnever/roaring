package roaring

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/RoaringBitmap/roaring/internal"
)

type container interface {
	Reset()

	// addOffset returns the (low, high) parts of the shifted container.
	// Whenever one of them would be empty, nil will be returned instead to
	// avoid unnecessary allocations.
	addOffset(uint16) (container, container)

	clone() container
	and(container) container
	andCardinality(container) int
	iand(container) container // i stands for inplace
	andNot(container) container
	iandNot(container) container // i stands for inplace
	isEmpty() bool
	getCardinality() int
	// rank returns the number of integers that are
	// smaller or equal to x. rank(infinity) would be getCardinality().
	rank(uint16) int

	iadd(x uint16) bool                   // inplace, returns true if x was new.
	iaddReturnMinimized(uint16) container // may change return type to minimize storage.

	//addRange(start, final int) container  // range is [firstOfRange,lastOfRange) (unused)
	iaddRange(start, endx int) container // i stands for inplace, range is [firstOfRange,endx)

	iremove(x uint16) bool                   // inplace, returns true if x was present.
	iremoveReturnMinimized(uint16) container // may change return type to minimize storage.

	not(start, final int) container        // range is [firstOfRange,lastOfRange)
	inot(firstOfRange, endx int) container // i stands for inplace, range is [firstOfRange,endx)
	xor(r container) container
	getShortIterator() shortPeekable
	iterate(cb func(x uint16) bool) bool
	getReverseIterator() shortIterable
	getManyIterator() manyIterable
	contains(i uint16) bool
	maximum() uint16
	minimum() uint16

	// equals is now logical equals; it does not require the
	// same underlying container types, but compares across
	// any of the implementations.
	equals(r container) bool

	fillLeastSignificant16bits(array []uint32, i int, mask uint32) int
	or(r container) container
	orCardinality(r container) int
	isFull() bool
	ior(r container) container   // i stands for inplace
	intersects(r container) bool // whether the two containers intersect
	lazyOR(r container) container
	lazyIOR(r container) container
	getSizeInBytes() int
	//removeRange(start, final int) container  // range is [firstOfRange,lastOfRange) (unused)
	iremoveRange(start, final int) container // i stands for inplace, range is [firstOfRange,lastOfRange)
	selectInt(x uint16) int                  // selectInt returns the xth integer in the container
	serializedSizeInBytes() int
	writeTo(io.Writer) (int, error)

	numberOfRuns() int
	toEfficientContainer() container
	String() string
	containerType() contype
}

type contype uint8

const (
	bitmapContype contype = iota
	arrayContype
	run16Contype
	run32Contype
)

// careful: range is [firstOfRange,lastOfRange]
func rangeOfOnes(start, last int) container {
	if start > MaxUint16 {
		panic("rangeOfOnes called with start > MaxUint16")
	}
	if last > MaxUint16 {
		panic("rangeOfOnes called with last > MaxUint16")
	}
	if start < 0 {
		panic("rangeOfOnes called with start < 0")
	}
	if last < 0 {
		panic("rangeOfOnes called with last < 0")
	}
	return newRunContainer16Range(uint16(start), uint16(last))
}

var _roaringArrayPool = internal.NewTypedPool(func() *roaringArray { return &roaringArray{} })

type roaringArray struct {
	keys       []uint16
	containers []container `msg:"-"` // don't try to serialize directly.
	unsafe     bool
}

func newRoaringArray() *roaringArray {
	return _roaringArrayPool.Get()
}

func (r *roaringArray) Reset() {
	r.recycle(0, len(r.containers))
	r.resize(0)
}

// runOptimize compresses the element containers to minimize space consumed.
// Q: how does this interact with copyOnWrite and needCopyOnWrite?
// A: since we aren't changing the logical content, just the representation,
//
//	we don't bother to check the needCopyOnWrite bits. We replace
//	(possibly all) elements of ra.containers in-place with space
//	optimized versions.
func (ra *roaringArray) runOptimize() {
	for i := range ra.containers {
		ra.containers[i] = ra.containers[i].toEfficientContainer()
	}
}

func (ra *roaringArray) appendContainer(key uint16, value container) {
	ra.keys = append(ra.keys, key)
	ra.containers = append(ra.containers, value)
}

func (ra *roaringArray) appendCopy(sa roaringArray, startingindex int) {
	ra.appendContainer(sa.keys[startingindex], sa.containers[startingindex].clone())
}

func (ra *roaringArray) appendCopyMany(sa roaringArray, startingindex, end int) {
	for i := startingindex; i < end; i++ {
		ra.appendCopy(sa, i)
	}
}

func (ra *roaringArray) appendCopiesUntil(sa roaringArray, stoppingKey uint16) {
	for i := 0; i < sa.size(); i++ {
		if sa.keys[i] >= stoppingKey {
			break
		}
		ra.appendContainer(sa.keys[i], sa.containers[i].clone())
	}
}

func (ra *roaringArray) appendCopiesAfter(sa roaringArray, beforeStart uint16) {
	startLocation := sa.getIndex(beforeStart)
	if startLocation >= 0 {
		startLocation++
	} else {
		startLocation = -startLocation - 1
	}

	for i := startLocation; i < sa.size(); i++ {
		ra.appendContainer(sa.keys[i], sa.containers[i].clone())
	}
}

func (ra *roaringArray) removeIndexRange(begin, end int) {
	if end <= begin {
		return
	}

	r := end - begin

	ra.recycle(begin, end)

	copy(ra.keys[begin:], ra.keys[end:])
	copy(ra.containers[begin:], ra.containers[end:])

	ra.resize(len(ra.keys) - r)
}

func (ra *roaringArray) recycle(start, end int) { // end not included.

	for i := start; i < end; i++ {
		c := ra.containers[i]
		ra.containers[i] = nil
		c.Reset()
		switch x := c.(type) {
		case *arrayContainer:
			_arrayContainerPool.Put(x)
		case *bitmapContainer:
			_bitmapContainerPool.Put(x)
		case *runContainer16:
			_runContainer16Pool.Put(x)
		}
	}
}

func (ra *roaringArray) resize(newsize int) {
	for k := newsize; k < len(ra.containers); k++ {
		ra.keys[k] = 0
		ra.containers[k] = nil
	}

	ra.keys = ra.keys[:newsize]
	ra.containers = ra.containers[:newsize]
}

func (ra *roaringArray) clear() {
	ra.Reset()
}

func (ra *roaringArray) clone() *roaringArray {
	sa := _roaringArrayPool.GetNoCreate()
	if sa != nil && cap(sa.keys) >= len(ra.keys) {
		sa.keys = sa.keys[:len(ra.keys)]
		sa.containers = sa.containers[:len(ra.containers)]
	} else {
		if sa != nil {
			_roaringArrayPool.Put(sa)
		}
		sa = &roaringArray{}
		sa.keys = make([]uint16, len(ra.keys))
		sa.containers = make([]container, len(ra.containers))
	}

	copy(sa.keys, ra.keys)
	for i := range sa.containers {
		sa.containers[i] = ra.containers[i].clone()
	}
	return sa
}

func (ra *roaringArray) Unsafe() bool {
	return ra.unsafe
}

// unused function:
//func (ra *roaringArray) containsKey(x uint16) bool {
//	return (ra.binarySearch(0, int64(len(ra.keys)), x) >= 0)
//}

func (ra *roaringArray) getContainer(x uint16) container {
	i := ra.binarySearch(0, int64(len(ra.keys)), x)
	if i < 0 {
		return nil
	}
	return ra.containers[i]
}

func (ra *roaringArray) getContainerAtIndex(i int) container {
	return ra.containers[i]
}

func (ra *roaringArray) getFastContainerAtIndex(i int, needsWriteable bool) container {
	c := ra.getContainerAtIndex(i)
	switch t := c.(type) {
	case *arrayContainer:
		c = t.toBitmapContainer()
	case *runContainer16:
		if !t.isFull() {
			c = t.toBitmapContainer()
		}
	case *bitmapContainer:
		c = ra.containers[i].clone()
	}
	return c
}

// getUnionedWritableContainer switches behavior for in-place Or
// depending on whether the container requires a copy on write.
// If it does using the non-inplace or() method leads to fewer allocations.
func (ra *roaringArray) getUnionedContainer(pos int, other container) container {
	return ra.getContainerAtIndex(pos).ior(other)
}

func (ra *roaringArray) getIndex(x uint16) int {
	// before the binary search, we optimize for frequent cases
	size := len(ra.keys)
	if (size == 0) || (ra.keys[size-1] == x) {
		return size - 1
	}
	return ra.binarySearch(0, int64(size), x)
}

func (ra *roaringArray) getKeyAtIndex(i int) uint16 {
	return ra.keys[i]
}

func (ra *roaringArray) insertNewKeyValueAt(i int, key uint16, value container) {
	ra.keys = append(ra.keys, 0)
	ra.containers = append(ra.containers, nil)

	copy(ra.keys[i+1:], ra.keys[i:])
	copy(ra.containers[i+1:], ra.containers[i:])

	ra.keys[i] = key
	ra.containers[i] = value
}

func (ra *roaringArray) remove(key uint16) bool {
	i := ra.binarySearch(0, int64(len(ra.keys)), key)
	if i >= 0 { // if a new key
		ra.removeAtIndex(i)
		return true
	}
	return false
}

func (ra *roaringArray) removeAtIndex(i int) {
	ra.recycle(i, i+1)

	copy(ra.keys[i:], ra.keys[i+1:])
	copy(ra.containers[i:], ra.containers[i+1:])

	ra.resize(len(ra.keys) - 1)
}

func (ra *roaringArray) setContainerAtIndex(i int, c container) {
	ra.containers[i] = c
}

func (ra *roaringArray) replaceKeyAndContainerAtIndex(i int, key uint16, c container) {
	ra.keys[i] = key
	ra.containers[i] = c
}

func (ra *roaringArray) size() int {
	return len(ra.keys)
}

func (ra *roaringArray) binarySearch(begin, end int64, ikey uint16) int {
	low := begin
	high := end - 1
	for low+16 <= high {
		middleIndex := low + (high-low)/2 // avoid overflow
		middleValue := ra.keys[middleIndex]

		if middleValue < ikey {
			low = middleIndex + 1
		} else if middleValue > ikey {
			high = middleIndex - 1
		} else {
			return int(middleIndex)
		}
	}
	for ; low <= high; low++ {
		val := ra.keys[low]
		if val >= ikey {
			if val == ikey {
				return int(low)
			}
			break
		}
	}
	return -int(low + 1)
}

func (ra *roaringArray) equals(o interface{}) bool {
	srb, ok := o.(roaringArray)
	if ok {

		if srb.size() != ra.size() {
			return false
		}
		for i, k := range ra.keys {
			if k != srb.keys[i] {
				return false
			}
		}

		for i, c := range ra.containers {
			if !c.equals(srb.containers[i]) {
				return false
			}
		}
		return true
	}
	return false
}

func (ra *roaringArray) headerSize() uint64 {
	size := uint64(len(ra.keys))
	if ra.hasRunCompression() {
		if size < noOffsetThreshold { // for small bitmaps, we omit the offsets
			return 4 + (size+7)/8 + 4*size
		}
		return 4 + (size+7)/8 + 8*size // - 4 because we pack the size with the cookie
	}
	return 4 + 4 + 8*size

}

// should be dirt cheap
func (ra *roaringArray) serializedSizeInBytes() uint64 {
	answer := ra.headerSize()
	for _, c := range ra.containers {
		answer += uint64(c.serializedSizeInBytes())
	}
	return answer
}

// spec: https://github.com/RoaringBitmap/RoaringFormatSpec
func (ra *roaringArray) writeTo(w io.Writer) (n int64, err error) {
	hasRun := ra.hasRunCompression()
	isRunSizeInBytes := 0
	cookieSize := 8
	if hasRun {
		cookieSize = 4
		isRunSizeInBytes = (len(ra.keys) + 7) / 8
	}
	descriptiveHeaderSize := 4 * len(ra.keys)
	preambleSize := cookieSize + isRunSizeInBytes + descriptiveHeaderSize

	buf := make([]byte, preambleSize+4*len(ra.keys))

	nw := 0

	if hasRun {
		binary.LittleEndian.PutUint16(buf[0:], uint16(serialCookie))
		nw += 2
		binary.LittleEndian.PutUint16(buf[2:], uint16(len(ra.keys)-1))
		nw += 2
		// compute isRun bitmap without temporary allocation
		var runbitmapslice = buf[nw : nw+isRunSizeInBytes]
		for i, c := range ra.containers {
			switch c.(type) {
			case *runContainer16:
				runbitmapslice[i/8] |= 1 << (uint(i) % 8)
			}
		}
		nw += isRunSizeInBytes
	} else {
		binary.LittleEndian.PutUint32(buf[0:], uint32(serialCookieNoRunContainer))
		nw += 4
		binary.LittleEndian.PutUint32(buf[4:], uint32(len(ra.keys)))
		nw += 4
	}

	// descriptive header
	for i, key := range ra.keys {
		binary.LittleEndian.PutUint16(buf[nw:], key)
		nw += 2
		c := ra.containers[i]
		binary.LittleEndian.PutUint16(buf[nw:], uint16(c.getCardinality()-1))
		nw += 2
	}

	startOffset := int64(preambleSize + 4*len(ra.keys))
	if !hasRun || (len(ra.keys) >= noOffsetThreshold) {
		// offset header
		for _, c := range ra.containers {
			binary.LittleEndian.PutUint32(buf[nw:], uint32(startOffset))
			nw += 4
			switch rc := c.(type) {
			case *runContainer16:
				startOffset += 2 + int64(len(rc.iv))*4
			default:
				startOffset += int64(getSizeInBytesFromCardinality(c.getCardinality()))
			}
		}
	}

	written, err := w.Write(buf[:nw])
	if err != nil {
		return n, err
	}
	n += int64(written)

	for _, c := range ra.containers {
		written, err := c.writeTo(w)
		if err != nil {
			return n, err
		}
		n += int64(written)
	}
	return n, nil
}

// spec: https://github.com/RoaringBitmap/RoaringFormatSpec
func (ra *roaringArray) toBytes() ([]byte, error) {
	var buf bytes.Buffer
	_, err := ra.writeTo(&buf)
	return buf.Bytes(), err
}

// Reads a serialized roaringArray from a byte slice.
func (ra *roaringArray) readFrom(stream internal.ByteInput, cookieHeader ...byte) (int64, error) {
	var cookie uint32
	var err error
	if len(cookieHeader) > 0 && len(cookieHeader) != 4 {
		return int64(len(cookieHeader)), fmt.Errorf("error in roaringArray.readFrom: could not read initial cookie: incorrect size of cookie header")
	}
	if len(cookieHeader) == 4 {
		cookie = binary.LittleEndian.Uint32(cookieHeader)
	} else {
		cookie, err = stream.ReadUInt32()
		if err != nil {
			return stream.GetReadBytes(), fmt.Errorf("error in roaringArray.readFrom: could not read initial cookie: %s", err)
		}
	}
	// If NextReturnsSafeSlice is false, then willNeedCopyOnWrite should be true
	ra.unsafe = !stream.NextReturnsSafeSlice()

	var size uint32
	var isRunBitmap []byte

	if cookie&0x0000FFFF == serialCookie {
		size = uint32(cookie>>16 + 1)
		// create is-run-container bitmap
		isRunBitmapSize := (int(size) + 7) / 8
		isRunBitmap, err = stream.Next(isRunBitmapSize)

		if err != nil {
			return stream.GetReadBytes(), fmt.Errorf("malformed bitmap, failed to read is-run bitmap, got: %s", err)
		}
	} else if cookie == serialCookieNoRunContainer {
		size, err = stream.ReadUInt32()
		if err != nil {
			return stream.GetReadBytes(), fmt.Errorf("malformed bitmap, failed to read a bitmap size: %s", err)
		}
	} else {
		return stream.GetReadBytes(), fmt.Errorf("error in roaringArray.readFrom: did not find expected serialCookie in header")
	}

	if size > (1 << 16) {
		return stream.GetReadBytes(), fmt.Errorf("it is logically impossible to have more than (1<<16) containers")
	}

	// descriptive header
	buf, err := stream.Next(2 * 2 * int(size))

	if err != nil {
		return stream.GetReadBytes(), fmt.Errorf("failed to read descriptive header: %s", err)
	}

	keycard := byteSliceAsUint16Slice(buf)

	if isRunBitmap == nil || size >= noOffsetThreshold {
		if err := stream.SkipBytes(int(size) * 4); err != nil {
			return stream.GetReadBytes(), fmt.Errorf("failed to skip bytes: %s", err)
		}
	}

	// Allocate slices upfront as number of containers is known
	if cap(ra.containers) >= int(size) {
		ra.containers = ra.containers[:size]
	} else {
		ra.containers = make([]container, size)
	}

	if cap(ra.keys) >= int(size) {
		ra.keys = ra.keys[:size]
	} else {
		ra.keys = make([]uint16, size)
	}

	for i := uint32(0); i < size; i++ {
		key := keycard[2*i]
		card := int(keycard[2*i+1]) + 1
		ra.keys[i] = key

		if isRunBitmap != nil && isRunBitmap[i/8]&(1<<(i%8)) != 0 {
			// run container
			nr, err := stream.ReadUInt16()

			if err != nil {
				return 0, fmt.Errorf("failed to read runtime container size: %s", err)
			}

			buf, err := stream.Next(int(nr) * 4)

			if err != nil {
				return stream.GetReadBytes(), fmt.Errorf("failed to read runtime container content: %s", err)
			}

			nb := runContainer16{
				iv: byteSliceAsInterval16Slice(buf),
			}

			ra.containers[i] = &nb
		} else if card > arrayDefaultMaxSize {
			// bitmap container
			buf, err := stream.Next(arrayDefaultMaxSize * 2)

			if err != nil {
				return stream.GetReadBytes(), fmt.Errorf("failed to read bitmap container: %s", err)
			}

			nb := bitmapContainer{
				cardinality: card,
				bitmap:      byteSliceAsUint64Slice(buf),
			}

			ra.containers[i] = &nb
		} else {
			// array container
			buf, err := stream.Next(card * 2)

			if err != nil {
				return stream.GetReadBytes(), fmt.Errorf("failed to read array container: %s", err)
			}

			nb := arrayContainer{
				byteSliceAsUint16Slice(buf),
			}

			ra.containers[i] = &nb
		}
	}

	return stream.GetReadBytes(), nil
}

func (ra *roaringArray) hasRunCompression() bool {
	for _, c := range ra.containers {
		switch c.(type) {
		case *runContainer16:
			return true
		}
	}
	return false
}

func (ra *roaringArray) advanceUntil(min uint16, pos int) int {
	lower := pos + 1

	if lower >= len(ra.keys) || ra.keys[lower] >= min {
		return lower
	}

	spansize := 1

	for lower+spansize < len(ra.keys) && ra.keys[lower+spansize] < min {
		spansize *= 2
	}
	var upper int
	if lower+spansize < len(ra.keys) {
		upper = lower + spansize
	} else {
		upper = len(ra.keys) - 1
	}

	if ra.keys[upper] == min {
		return upper
	}

	if ra.keys[upper] < min {
		// means
		// array
		// has no
		// item
		// >= min
		// pos = array.length;
		return len(ra.keys)
	}

	// we know that the next-smallest span was too small
	lower += (spansize >> 1)

	mid := 0
	for lower+1 != upper {
		mid = (lower + upper) >> 1
		if ra.keys[mid] == min {
			return mid
		} else if ra.keys[mid] < min {
			lower = mid
		} else {
			upper = mid
		}
	}
	return upper
}
