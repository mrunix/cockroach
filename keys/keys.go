// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package keys

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
)

// MakeKey makes a new key which is the concatenation of the
// given inputs, in order.
func MakeKey(keys ...[]byte) []byte {
	return roachpb.MakeKey(keys...)
}

// MakeStoreKey creates a store-local key based on the metadata key
// suffix, and optional detail.
func MakeStoreKey(suffix, detail roachpb.RKey) roachpb.Key {
	return roachpb.Key(MakeKey(localStorePrefix, suffix, detail))
}

// StoreIdentKey returns a store-local key for the store metadata.
func StoreIdentKey() roachpb.Key {
	return MakeStoreKey(localStoreIdentSuffix, roachpb.RKey{})
}

// StoreStatusKey returns the key for accessing the store status for the
// specified store ID.
func StoreStatusKey(storeID int32) roachpb.Key {
	return roachpb.Key(MakeKey(StatusStorePrefix, encoding.EncodeUvarint(nil, uint64(storeID))))
}

// NodeStatusKey returns the key for accessing the node status for the
// specified node ID.
func NodeStatusKey(nodeID int32) roachpb.Key {
	return MakeKey(StatusNodePrefix, encoding.EncodeUvarint(nil, uint64(nodeID)))
}

// MakeRangeIDPrefix creates a range-local key prefix from
// rangeID.
func MakeRangeIDPrefix(rangeID roachpb.RangeID) roachpb.Key {
	return MakeKey(LocalRangeIDPrefix, encoding.EncodeUvarint(nil, uint64(rangeID)))
}

// MakeRangeIDKey creates a range-local key based on the range's
// Range ID, metadata key suffix, and optional detail.
func MakeRangeIDKey(rangeID roachpb.RangeID, suffix, detail roachpb.RKey) roachpb.Key {
	if len(suffix) != localSuffixLength {
		panic(fmt.Sprintf("suffix len(%q) != %d", suffix, localSuffixLength))
	}
	return MakeKey(MakeRangeIDPrefix(rangeID), suffix, detail)
}

// RaftLogKey returns a system-local key for a Raft log entry.
func RaftLogKey(rangeID roachpb.RangeID, logIndex uint64) roachpb.Key {
	return MakeRangeIDKey(rangeID, localRaftLogSuffix,
		encoding.EncodeUint64(nil, logIndex))
}

// RaftLogPrefix returns the system-local prefix shared by all entries in a Raft log.
func RaftLogPrefix(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDKey(rangeID, localRaftLogSuffix, roachpb.RKey{})
}

// RaftHardStateKey returns a system-local key for a Raft HardState.
func RaftHardStateKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDKey(rangeID, localRaftHardStateSuffix, roachpb.RKey{})
}

// RaftTruncatedStateKey returns a system-local key for a RaftTruncatedState.
func RaftTruncatedStateKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDKey(rangeID, localRaftTruncatedStateSuffix, roachpb.RKey{})
}

// RaftAppliedIndexKey returns a system-local key for a raft applied index.
func RaftAppliedIndexKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDKey(rangeID, localRaftAppliedIndexSuffix, roachpb.RKey{})
}

// RaftLeaderLeaseKey returns a system-local key for a raft leader lease.
func RaftLeaderLeaseKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDKey(rangeID, localRaftLeaderLeaseSuffix, roachpb.RKey{})
}

// RaftTombstoneKey returns a system-local key for a raft tombstone.
func RaftTombstoneKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDKey(rangeID, localRaftTombstoneSuffix, roachpb.RKey{})
}

// RaftLastIndexKey returns a system-local key for a raft last index.
func RaftLastIndexKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDKey(rangeID, localRaftLastIndexSuffix, roachpb.RKey{})
}

// RangeStatsKey returns the key for accessing the MVCCStats struct
// for the specified Range ID.
func RangeStatsKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDKey(rangeID, localRangeStatsSuffix, roachpb.RKey{})
}

// SequenceCacheKey returns a range-local key by Range ID for a
// sequence cache entry, with detail specified by encoding the
// supplied transaction ID, epoch and sequence number.
func SequenceCacheKey(rangeID roachpb.RangeID, id []byte, epoch uint32, seq uint32) roachpb.Key {
	return MakeRangeIDKey(rangeID, LocalSequenceCacheSuffix,
		encoding.EncodeUint32Decreasing(
			encoding.EncodeUint32Decreasing(
				encoding.EncodeBytes(nil, id), epoch), seq))
}

// SequenceCacheKeyPrefix returns the prefix common to all sequence cache keys
// for the given ID.
func SequenceCacheKeyPrefix(rangeID roachpb.RangeID, id []byte) roachpb.Key {
	return MakeRangeIDKey(rangeID, LocalSequenceCacheSuffix, encoding.EncodeBytes(nil, id))
}

// MakeRangeKey creates a range-local key based on the range
// start key, metadata key suffix, and optional detail (e.g. the
// transaction ID for a txn record, etc.).
func MakeRangeKey(key, suffix, detail roachpb.RKey) roachpb.Key {
	if len(suffix) != localSuffixLength {
		panic(fmt.Sprintf("suffix len(%q) != %d", suffix, localSuffixLength))
	}
	return MakeKey(MakeRangeKeyPrefix(key), suffix, detail)
}

// MakeRangeKeyPrefix creates a key prefix under which all range-local keys
// can be found.
func MakeRangeKeyPrefix(key roachpb.RKey) roachpb.Key {
	return MakeKey(LocalRangePrefix, encoding.EncodeBytes(nil, key))
}

// DecodeRangeKey decodes the range key into range start key,
// suffix and optional detail (may be nil).
func DecodeRangeKey(key roachpb.Key) (startKey, suffix, detail roachpb.Key, err error) {
	if !bytes.HasPrefix(key, LocalRangePrefix) {
		return nil, nil, nil, util.Errorf("key %q does not have %q prefix",
			key, LocalRangePrefix)
	}
	// Cut the prefix and the Range ID.
	b := key[len(LocalRangePrefix):]
	b, startKey, err = encoding.DecodeBytes(b, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(b) < localSuffixLength {
		return nil, nil, nil, util.Errorf("key %q does not have suffix of length %d",
			key, localSuffixLength)
	}
	// Cut the suffix.
	suffix = b[:localSuffixLength]
	detail = b[localSuffixLength:]
	return
}

// RangeLastVerificationTimestampKey returns a range-local key for
// the range's last verification timestamp.
func RangeLastVerificationTimestampKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDKey(rangeID, localRangeLastVerificationTimestampSuffix, roachpb.RKey{})
}

// RangeTreeNodeKey returns a range-local key for the range's
// node in the range tree.
func RangeTreeNodeKey(key roachpb.RKey) roachpb.Key {
	return MakeRangeKey(key, localRangeTreeNodeSuffix, roachpb.RKey{})
}

// RangeDescriptorKey returns a range-local key for the descriptor
// for the range with specified key.
func RangeDescriptorKey(key roachpb.RKey) roachpb.Key {
	return MakeRangeKey(key, LocalRangeDescriptorSuffix, roachpb.RKey{})
}

// TransactionKey returns a transaction key based on the provided
// transaction key and ID. The base key is encoded in order to
// guarantee that all transaction records for a range sort together.
func TransactionKey(key roachpb.Key, id []byte) roachpb.Key {
	return MakeRangeKey(Addr(key), localTransactionSuffix, roachpb.RKey(id))
}

// Addr returns the address for the key, used to lookup the range containing
// the key. In the normal case, this is simply the key's value. However, for
// local keys, such as transaction records, range-spanning binary tree node
// pointers, the address is the trailing suffix of the key, with the local key
// prefix removed. In this way, local keys address to the same range as
// non-local keys, but are stored separately so that they don't collide with
// user-space or global system keys.
//
// However, not all local keys are addressable in the global map. Only range
// local keys incorporating a range key (start key or transaction key) are
// addressable (e.g. range metadata and txn records). Range local keys
// incorporating the Range ID are not (e.g. sequence cache entries, and range
// stats).
//
// TODO(pmattis): Should KeyAddress return an error when the key is malformed?
func Addr(k roachpb.Key) roachpb.RKey {
	if k == nil {
		return nil
	}

	if !bytes.HasPrefix(k, localPrefix) {
		return roachpb.RKey(k)
	}
	if bytes.HasPrefix(k, LocalRangePrefix) {
		k = k[len(LocalRangePrefix):]
		_, k, err := encoding.DecodeBytes(k, nil)
		if err != nil {
			panic(err)
		}
		return roachpb.RKey(k)
	}
	log.Fatalf("local key %q malformed; should contain prefix %q",
		k, LocalRangePrefix)
	return nil
}

// RangeMetaKey returns a range metadata (meta1, meta2) indexing key
// for the given key. For ordinary keys this returns a level 2
// metadata key - for level 2 keys, it returns a level 1 key. For
// level 1 keys and local keys, KeyMin is returned.
func RangeMetaKey(key roachpb.RKey) roachpb.Key {
	if len(key) == 0 {
		return roachpb.KeyMin
	}
	switch key[0] {
	case Meta1Prefix[0]:
		return roachpb.KeyMin
	case Meta2Prefix[0]:
		return MakeKey(Meta1Prefix, key[len(Meta2Prefix):])
	default:
		return MakeKey(Meta2Prefix, key)
	}
}

// validateRangeMetaKey validates that the given key is a valid Range Metadata
// key. This checks only the constraints common to forward and backwards scans:
// correct prefix and not exceeding KeyMax.
func validateRangeMetaKey(key roachpb.RKey) error {
	// KeyMin is a valid key.
	if key.Equal(roachpb.RKeyMin) {
		return nil
	}
	// Key must be at least as long as Meta1Prefix.
	if len(key) < len(Meta1Prefix) {
		return NewInvalidRangeMetaKeyError("too short", key)
	}

	prefix, body := key[:len(Meta1Prefix)], key[len(Meta1Prefix):]
	if !prefix.Equal(Meta2Prefix) && !prefix.Equal(Meta1Prefix) {
		return NewInvalidRangeMetaKeyError("not a meta key", key)
	}

	if roachpb.RKeyMax.Less(body) {
		return NewInvalidRangeMetaKeyError("body of meta key range lookup is > KeyMax", key)
	}
	return nil
}

// MetaScanBounds returns the range [start,end) within which the desired meta
// record can be found by means of an engine scan. The given key must be a
// valid RangeMetaKey as defined by validateRangeMetaKey.
// TODO(tschottdorf): a lot of casting going on inside.
func MetaScanBounds(key roachpb.RKey) (roachpb.Key, roachpb.Key, error) {
	if err := validateRangeMetaKey(key); err != nil {
		return nil, nil, err
	}

	if key.Equal(Meta2KeyMax) {
		return nil, nil, NewInvalidRangeMetaKeyError("Meta2KeyMax can't be used as the key of scan", key)
	}

	if key.Equal(roachpb.RKeyMin) {
		// Special case KeyMin: find the first entry in meta1.
		return Meta1Prefix, Meta1Prefix.PrefixEnd(), nil
	}
	if key.Equal(Meta1KeyMax) {
		// Special case Meta1KeyMax: this is the last key in Meta1, we don't want
		// to start at Next().
		return Meta1KeyMax, Meta1Prefix.PrefixEnd(), nil
	}
	// Otherwise find the first entry greater than the given key in the same meta prefix.
	return key.Next().AsRawKey(), key[:len(Meta1Prefix)].PrefixEnd().AsRawKey(), nil
}

// MetaReverseScanBounds returns the range [start,end) within which the desired
// meta record can be found by means of a reverse engine scan. The given key
// must be a valid RangeMetaKey as defined by validateRangeMetaKey.
func MetaReverseScanBounds(key roachpb.RKey) (roachpb.Key, roachpb.Key, error) {
	if err := validateRangeMetaKey(key); err != nil {
		return nil, nil, err
	}

	if key.Equal(roachpb.RKeyMin) || key.Equal(Meta1Prefix) {
		return nil, nil, NewInvalidRangeMetaKeyError("KeyMin and Meta1Prefix can't be used as the key of reverse scan", key)
	}
	if key.Equal(Meta2Prefix) {
		// Special case Meta2Prefix: this is the first key in Meta2, and the scan
		// interval covers all of Meta1.
		return Meta1Prefix, key.Next().AsRawKey(), nil
	}
	// Otherwise find the first entry greater than the given key and find the last entry
	// in the same prefix. For MVCCReverseScan the endKey is exclusive, if we want to find
	// the range descriptor the given key specified,we need to set the key.Next() as the
	// MVCCReverseScan`s endKey. For example:
	// If we have ranges [a,f) and [f,z), then we'll have corresponding meta records
	// at f and z. If you're looking for the meta record for key f, then you want the
	// second record (exclusive in MVCCReverseScan), hence key.Next() below.
	return key[:len(Meta1Prefix)].AsRawKey(), key.Next().AsRawKey(), nil
}

// MakeTablePrefix returns the key prefix used for the table's data.
func MakeTablePrefix(tableID uint32) []byte {
	return encoding.EncodeUvarint(nil, uint64(tableID))
}

// DecodeTablePrefix validates that the given key has a table prefix, returning
// the remainder of the key (with the prefix removed) and the decoded descriptor
// ID of the table.
func DecodeTablePrefix(key roachpb.Key) ([]byte, uint64, error) {
	if encoding.PeekType(key) != encoding.Int {
		return key, 0, util.Errorf("invalid key prefix: %q", key)
	}
	return encoding.DecodeUvarint(key)
}

// MakeColumnKey returns the key for the column in the given row.
func MakeColumnKey(rowKey []byte, colID uint32) []byte {
	key := append([]byte(nil), rowKey...)
	size := len(key)
	key = encoding.EncodeUvarint(key, uint64(colID))
	// Note that we assume that `len(key)-size` will always be encoded to a
	// single byte by EncodeUvarint. This is currently always true because the
	// varint encoding will encode 1-9 bytes.
	return encoding.EncodeUvarint(key, uint64(len(key)-size))
}

// MakeNonColumnKey creates a non-column key for a row by appending a 0 column
// ID suffix size to rowKey.
func MakeNonColumnKey(rowKey []byte) []byte {
	return encoding.EncodeUvarint(rowKey, 0)
}

// MakeSplitKey transforms an SQL table key such that it is a valid split key
// (i.e. does not occur in the middle of a row).
func MakeSplitKey(key roachpb.Key) (roachpb.Key, error) {
	if encoding.PeekType(key) != encoding.Int {
		// Not a table key, so already a split key.
		return key, nil
	}

	n := len(key)
	// The column ID length is encoded as a varint and we take advantage of the
	// fact that the column ID itself will be encoded in 0-9 bytes and thus the
	// length of the column ID data will fit in a single byte.
	buf := key[n-1:]
	if encoding.PeekType(buf) != encoding.Int {
		// The last byte is not a valid column ID suffix.
		return nil, util.Errorf("%s: not a valid table key", key)
	}

	// Strip off the column ID suffix from the buf. The last byte of the buf
	// contains the length of the column ID suffix (which might be 0 if the buf
	// does not contain a column ID suffix).
	_, colIDLen, err := encoding.DecodeUvarint(buf)
	if err != nil {
		return nil, err
	}
	if int(colIDLen)+1 > n {
		// The column ID length was impossible. colIDLen is the length of the
		// encoded column ID suffix. We add 1 to account for the byte holding the
		// length of the encoded column ID and if that total (colIDLen+1) is
		// greater than the key suffix (n == len(buf)) then we bail. Note that we
		// don't consider this an error because MakeSplitKey can be called on keys
		// that look like table keys but which do not have a column ID length
		// suffix (e.g SystemConfig.ComputeSplitKeys).
		return nil, util.Errorf("%s: malformed table key", key)
	}
	return key[:len(key)-int(colIDLen)-1], nil
}

// Range returns a key range encompassing all the keys in the Batch.
// TODO(tschottdorf): there is no protection for doubly-local keys here;
// maybe Range should return an error.
func Range(ba roachpb.BatchRequest) roachpb.RSpan {
	from := roachpb.RKeyMax
	to := roachpb.RKeyMin
	for _, arg := range ba.Requests {
		req := arg.GetInner()
		if req.Method() == roachpb.Noop {
			continue
		}
		h := req.Header()
		key := Addr(h.Key)
		if key.Less(from) {
			// Key is smaller than `from`.
			from = key
		}
		if !key.Less(to) {
			// Key.Next() is larger than `to`.
			to = key.Next()
		}
		if endKey := Addr(h.EndKey); to.Less(endKey) {
			// EndKey is larger than `to`.
			to = endKey
		}
	}
	return roachpb.RSpan{Key: from, EndKey: to}
}
