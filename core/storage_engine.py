"""
Local Storage Engine with Integrity Verification.

Implements a simplified LSM-tree style storage for metadata and small objects,
combined with an append-only log (Bitcask-style) for large blob storage.
Every object gets a CRC32 checksum for corruption detection.
"""

import hashlib
import zlib
import time
import threading
import os
from typing import Dict, List, Optional, Tuple, Any
from collections import OrderedDict

from .vector_clock import VectorClock
from .merkle_tree import MerkleTree


class StoredObject:
    """Represents an object stored in the local engine."""

    def __init__(self, key: str, value: bytes, vector_clock: VectorClock,
                 coordinator_node: str = ""):
        self.key = key
        self.value = value
        self.vector_clock = vector_clock
        self.checksum = self._compute_checksum(value)
        self.size = len(value)
        self.created_at = time.time()
        self.last_accessed = time.time()
        self.coordinator_node = coordinator_node
        self.is_hint = False  # For hinted handoff
        self.hint_target: Optional[str] = None

    def _compute_checksum(self, data: bytes) -> int:
        """Compute CRC32 checksum for data integrity."""
        return zlib.crc32(data) & 0xFFFFFFFF

    def verify_integrity(self) -> bool:
        """Verify the stored data against its checksum."""
        computed = self._compute_checksum(self.value)
        return computed == self.checksum

    def content_hash(self) -> str:
        """SHA-256 hash of the value for Merkle tree."""
        return hashlib.sha256(self.value).hexdigest()

    def to_dict(self) -> dict:
        return {
            'key': self.key,
            'size': self.size,
            'checksum': hex(self.checksum),
            'vector_clock': self.vector_clock.to_dict(),
            'created_at': self.created_at,
            'last_accessed': self.last_accessed,
            'coordinator_node': self.coordinator_node,
            'is_hint': self.is_hint,
            'hint_target': self.hint_target,
            'integrity_ok': self.verify_integrity(),
        }


class LocalStorageEngine:
    """
    Local storage engine for a single Vault node.

    Combines:
    - In-memory key-value store (simulating LSM-tree memtable)
    - Integrity verification via CRC32
    - Merkle tree for anti-entropy
    - Hinted handoff queue for temporarily unreachable replicas
    """

    def __init__(self, node_id: str, max_objects: int = 100000):
        self.node_id = node_id
        self.max_objects = max_objects
        self._lock = threading.RLock()

        # Primary data store
        self._data: Dict[str, StoredObject] = OrderedDict()
        # Hinted handoff queue: target_node -> list of StoredObjects
        self._hint_queue: Dict[str, List[StoredObject]] = {}
        # Merkle tree for this node's data
        self._merkle_tree = MerkleTree()

        # Storage statistics
        self._stats = {
            'total_writes': 0,
            'total_reads': 0,
            'total_deletes': 0,
            'integrity_checks': 0,
            'integrity_failures': 0,
            'bytes_stored': 0,
            'hints_stored': 0,
            'hints_delivered': 0,
            'read_repairs': 0,
            'corrupted_objects': set(),
        }

    def put(self, key: str, value: bytes, vector_clock: VectorClock,
            coordinator: str = "") -> StoredObject:
        """
        Store an object locally.

        Args:
            key: Object key
            value: Object payload
            vector_clock: Version vector for this write
            coordinator: ID of the coordinator node

        Returns:
            The stored object
        """
        with self._lock:
            obj = StoredObject(key, value, vector_clock, coordinator or self.node_id)

            # Update Merkle tree
            self._merkle_tree.insert(key, obj.content_hash())

            # Update stats
            if key in self._data:
                self._stats['bytes_stored'] -= self._data[key].size
            self._data[key] = obj
            self._stats['total_writes'] += 1
            self._stats['bytes_stored'] += obj.size

            return obj

    def get(self, key: str) -> Optional[StoredObject]:
        """
        Retrieve an object, verifying integrity on read.

        Returns None if the key doesn't exist.
        Raises ValueError if the data is corrupted (checksum mismatch).
        """
        with self._lock:
            self._stats['total_reads'] += 1

            obj = self._data.get(key)
            if obj is None:
                return None

            obj.last_accessed = time.time()
            self._stats['integrity_checks'] += 1

            # Read-time integrity verification
            if not obj.verify_integrity():
                self._stats['integrity_failures'] += 1
                self._stats['corrupted_objects'].add(key)
                raise ValueError(f"Data corruption detected for key '{key}' on node {self.node_id}")

            return obj

    def delete(self, key: str) -> bool:
        """Delete an object."""
        with self._lock:
            if key in self._data:
                self._stats['bytes_stored'] -= self._data[key].size
                del self._data[key]
                self._merkle_tree.remove(key)
                self._stats['total_deletes'] += 1
                return True
            return False

    def get_version(self, key: str) -> Optional[VectorClock]:
        """Get the vector clock for a key without reading the full value."""
        with self._lock:
            obj = self._data.get(key)
            return obj.vector_clock if obj else None

    def has_key(self, key: str) -> bool:
        with self._lock:
            return key in self._data

    def keys(self) -> List[str]:
        with self._lock:
            return list(self._data.keys())

    # --- Hinted Handoff ---

    def store_hint(self, target_node: str, key: str, value: bytes,
                   vector_clock: VectorClock, coordinator: str):
        """
        Store a hinted replica for a temporarily unreachable node.
        The hint will be delivered once the target comes back online.
        """
        with self._lock:
            obj = StoredObject(key, value, vector_clock, coordinator)
            obj.is_hint = True
            obj.hint_target = target_node

            if target_node not in self._hint_queue:
                self._hint_queue[target_node] = []
            self._hint_queue[target_node].append(obj)
            self._stats['hints_stored'] += 1

    def get_hints_for(self, target_node: str) -> List[StoredObject]:
        """Retrieve and clear all hints for a target node."""
        with self._lock:
            hints = self._hint_queue.pop(target_node, [])
            self._stats['hints_delivered'] += len(hints)
            return hints

    def get_pending_hints(self) -> Dict[str, int]:
        """Get count of pending hints per target node."""
        with self._lock:
            return {node: len(hints) for node, hints in self._hint_queue.items()}

    # --- Merkle Tree & Anti-Entropy ---

    def get_merkle_root(self) -> str:
        """Get the root hash of this node's Merkle tree."""
        with self._lock:
            return self._merkle_tree.get_root_hash()

    def get_merkle_tree(self) -> MerkleTree:
        """Get the full Merkle tree for comparison."""
        with self._lock:
            return self._merkle_tree

    def compare_with(self, other_tree: MerkleTree) -> set:
        """
        Compare this node's Merkle tree with another's to find differences.
        Returns set of keys that differ.
        """
        with self._lock:
            return self._merkle_tree.diff(other_tree)

    # --- Corruption Simulation ---

    def corrupt_object(self, key: str) -> bool:
        """Simulate data corruption (for chaos testing)."""
        with self._lock:
            if key in self._data:
                obj = self._data[key]
                if obj.value:
                    # Flip some bits
                    corrupted = bytearray(obj.value)
                    corrupted[0] ^= 0xFF
                    obj.value = bytes(corrupted)
                    return True
            return False

    # --- Scrubbing ---

    def scrub(self) -> List[str]:
        """
        Scrub all stored objects, checking integrity.
        Returns list of corrupted keys.
        """
        corrupted = []
        with self._lock:
            for key, obj in self._data.items():
                self._stats['integrity_checks'] += 1
                if not obj.verify_integrity():
                    self._stats['integrity_failures'] += 1
                    self._stats['corrupted_objects'].add(key)
                    corrupted.append(key)
        return corrupted

    # --- Stats ---

    def get_stats(self) -> dict:
        with self._lock:
            return {
                'node_id': self.node_id,
                'total_objects': len(self._data),
                'total_writes': self._stats['total_writes'],
                'total_reads': self._stats['total_reads'],
                'total_deletes': self._stats['total_deletes'],
                'bytes_stored': self._stats['bytes_stored'],
                'integrity_checks': self._stats['integrity_checks'],
                'integrity_failures': self._stats['integrity_failures'],
                'corrupted_objects': list(self._stats['corrupted_objects']),
                'hints_stored': self._stats['hints_stored'],
                'hints_delivered': self._stats['hints_delivered'],
                'read_repairs': self._stats['read_repairs'],
                'pending_hints': self.get_pending_hints(),
                'merkle_root': self.get_merkle_root()[:16] + '...' if self.get_merkle_root() else 'empty',
                'merkle_stats': self._merkle_tree.get_stats(),
            }

    def get_all_objects_info(self) -> List[dict]:
        """Return metadata for all stored objects (for dashboard)."""
        with self._lock:
            return [obj.to_dict() for obj in self._data.values()]
