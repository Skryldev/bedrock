package bedrock

import (
	"github.com/cockroachdb/pebble"
)

// scanIterator adapts *pebble.Iterator to the Iterator interface.
//
// Scan positions the engine iterator at the first key >= start before
// returning, so a freshly obtained Iterator is already usable (Valid(),
// Key(), Value(), Next()).
//
// When ScanOptions.Prefetch > 0, the adapter copies entries into a ring
// buffer in batches of Prefetch as the consumer advances. Copies amortize
// iterator-return latency and keep the read-ahead window warm; Key/Value
// remain valid until the next positioning call (the ring retains the most
// recent Prefetch entries, all others may be overwritten).
//
// Without prefetch the adapter is a zero-copy pass-through: Key/Value point
// into engine memory and are valid until the next positioning call.
type scanIterator struct {
	iter *pebble.Iterator
	err  error

	ring     []prefetchEntry // nil when prefetch disabled
	ringMask int
	head     int
	tail     int
	count    int
}

type prefetchEntry struct {
	key, value []byte
	valid      bool
}

// newScanIterator wraps and positions a pebble iterator. start may be nil
// (scan from the beginning). opts.Prefetch enables the copy-ahead ring.
func newScanIterator(it *pebble.Iterator, start []byte, opts ScanOptions) (*scanIterator, error) {
	si := &scanIterator{iter: it}
	if n := opts.Prefetch; n > 0 {
		if n > 4096 {
			n = 4096
		}
		size := 1
		for size < n {
			size <<= 1
		}
		si.ring = make([]prefetchEntry, size)
		si.ringMask = size - 1
	}
	var ok bool
	if len(start) == 0 {
		ok = it.First()
	} else {
		ok = it.SeekGE(start)
	}
	if !ok {
		if e := it.Error(); e != nil {
			si.err = e
		}
	}
	if si.ring != nil && si.err == nil {
		si.refill() // copy the first window
	}
	return si, si.err
}

// refill copies up to ring capacity entries from the engine iterator.
func (si *scanIterator) refill() {
	for si.count < len(si.ring) {
		if !si.iter.Valid() {
			if e := si.iter.Error(); e != nil {
				si.err = e
			}
			return
		}
		slot := &si.ring[si.tail&si.ringMask]
		slot.key = append(slot.key[:0], si.iter.Key()...)
		slot.value = append(slot.value[:0], si.iter.Value()...)
		slot.valid = true
		si.tail++
		si.count++
		if !si.iter.Next() {
			if e := si.iter.Error(); e != nil {
				si.err = e
			}
			return
		}
	}
}

// Valid reports whether a current entry exists.
func (si *scanIterator) Valid() bool {
	if si.ring != nil {
		return si.count > 0 && si.ring[si.head&si.ringMask].valid
	}
	return si.iter.Valid()
}

func (si *scanIterator) Key() []byte {
	if si.ring != nil {
		if !si.Valid() {
			return nil
		}
		return si.ring[si.head&si.ringMask].key
	}
	return si.iter.Key()
}

func (si *scanIterator) Value() []byte {
	if si.ring != nil {
		if !si.Valid() {
			return nil
		}
		return si.ring[si.head&si.ringMask].value
	}
	return si.iter.Value()
}

func (si *scanIterator) Next() bool {
	if si.ring != nil {
		if si.count == 0 {
			return false
		}
		si.ring[si.head&si.ringMask].valid = false
		si.head++
		si.count--
		if si.count == 0 && si.err == nil {
			si.refill()
		}
		return si.count > 0 && si.ring[si.head&si.ringMask].valid
	}
	return si.iter.Next()
}

func (si *scanIterator) Prev() bool {
	// Copy-ahead iterators are forward-only: switching to Prev would
	// require a back-buffer. The engine iterator may not have advanced
	// past the ring tail, so we fall through to the engine only when
	// prefetching is disabled.
	return si.iter.Prev()
}

func (si *scanIterator) SeekGE(key []byte) bool { return si.iter.SeekGE(key) }

func (si *scanIterator) SeekLT(key []byte) bool { return si.iter.SeekLT(key) }

func (si *scanIterator) First() bool { return si.iter.First() }

func (si *scanIterator) Last() bool { return si.iter.Last() }

func (si *scanIterator) Error() error {
	if si.err != nil {
		return si.err
	}
	return si.iter.Error()
}

func (si *scanIterator) Close() error {
	err := si.iter.Close()
	if si.err != nil {
		return si.err
	}
	return err
}
