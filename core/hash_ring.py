"""
Consistent Hashing with Virtual Nodes (vnodes)

Implements a hash ring for distributed data partitioning. Each physical node
is mapped to multiple virtual nodes (vnodes) for even data distribution and
minimal rebalancing during topology changes.

Uses MurmurHash3-inspired hashing (via hashlib/md5 for portability).
"""

import hashlib
import bisect
from typing import List, Tuple, Optional, Dict, Set
import threading
import time
import math


def murmurhash3(key: str) -> int:
    """
    Fast hash function for consistent hashing ring placement.
    Uses MD5 internally for deterministic 128-bit hashing, then takes
    the first 8 bytes as an integer for ring position.
    """
    h = hashlib.md5(key.encode('utf-8')).digest()
    return int.from_bytes(h[:8], 'big')


class VirtualNode:
    """Represents a virtual node on the hash ring."""
    __slots__ = ['physical_node_id', 'vnode_index', 'position']

    def __init__(self, physical_node_id: str, vnode_index: int, position: int):
        self.physical_node_id = physical_node_id
        self.vnode_index = vnode_index
        self.position = position

    def __repr__(self):
        return f"VNode({self.physical_node_id}:{self.vnode_index} @{self.position:#x})"


class ConsistentHashRing:
    """
    Consistent Hashing Ring with Virtual Nodes.

    The hash space is treated as a continuous circular ring [0, 2^64).
    Each physical node is assigned `vnodes_per_node` virtual nodes evenly
    distributed around the ring. Objects are mapped to their coordinator
    by walking clockwise from the object's hash position.
    """

    RING_SIZE = 2**64

    def __init__(self, vnodes_per_node: int = 256):
        self.vnodes_per_node = vnodes_per_node
        self._lock = threading.RLock()

        # Sorted list of ring positions for binary search
        self._ring_positions: List[int] = []
        # Map from ring position -> VirtualNode
        self._position_to_vnode: Dict[int, VirtualNode] = {}
        # Map from physical node id -> list of VirtualNodes
        self._node_vnodes: Dict[str, List[VirtualNode]] = {}
        # Set of active physical node IDs
        self._active_nodes: Set[str] = set()

    @property
    def nodes(self) -> List[str]:
        """Return list of active physical node IDs."""
        with self._lock:
            return list(self._active_nodes)

    @property
    def total_vnodes(self) -> int:
        with self._lock:
            return len(self._ring_positions)

    def _hash_vnode(self, node_id: str, vnode_index: int) -> int:
        """Generate deterministic ring position for a virtual node."""
        key = f"{node_id}:vnode:{vnode_index}"
        return murmurhash3(key) % self.RING_SIZE

    def add_node(self, node_id: str) -> List[VirtualNode]:
        """
        Add a physical node to the ring with `vnodes_per_node` virtual nodes.
        Returns the list of created virtual nodes.
        """
        with self._lock:
            if node_id in self._active_nodes:
                return self._node_vnodes.get(node_id, [])

            vnodes = []
            for i in range(self.vnodes_per_node):
                position = self._hash_vnode(node_id, i)
                # Handle extremely rare hash collisions
                while position in self._position_to_vnode:
                    position = (position + 1) % self.RING_SIZE

                vnode = VirtualNode(node_id, i, position)
                vnodes.append(vnode)

                bisect.insort(self._ring_positions, position)
                self._position_to_vnode[position] = vnode

            self._node_vnodes[node_id] = vnodes
            self._active_nodes.add(node_id)
            return vnodes

    def remove_node(self, node_id: str) -> List[VirtualNode]:
        """
        Remove a physical node and all its vnodes from the ring.
        Returns the removed virtual nodes for rebalancing.
        """
        with self._lock:
            if node_id not in self._active_nodes:
                return []

            removed = self._node_vnodes.pop(node_id, [])
            self._active_nodes.discard(node_id)

            for vnode in removed:
                self._ring_positions.remove(vnode.position)
                del self._position_to_vnode[vnode.position]

            return removed

    def get_node(self, key: str) -> Optional[str]:
        """
        Find the coordinator node for a given key by walking clockwise.
        Returns the physical node ID, or None if the ring is empty.
        """
        with self._lock:
            if not self._ring_positions:
                return None

            hash_val = murmurhash3(key) % self.RING_SIZE
            idx = bisect.bisect_right(self._ring_positions, hash_val)

            # Wrap around to the beginning of the ring
            if idx >= len(self._ring_positions):
                idx = 0

            position = self._ring_positions[idx]
            return self._position_to_vnode[position].physical_node_id

    def get_replica_nodes(self, key: str, n: int) -> List[str]:
        """
        Get N distinct physical nodes for replication by walking clockwise.

        Ensures replicas are placed on distinct physical nodes (not just
        distinct vnodes on the same machine).
        """
        with self._lock:
            if not self._ring_positions:
                return []

            result = []
            seen_physical = set()

            hash_val = murmurhash3(key) % self.RING_SIZE
            idx = bisect.bisect_right(self._ring_positions, hash_val)

            # Walk the ring clockwise, collecting distinct physical nodes
            total_vnodes = len(self._ring_positions)
            for i in range(total_vnodes):
                ring_idx = (idx + i) % total_vnodes
                position = self._ring_positions[ring_idx]
                vnode = self._position_to_vnode[position]
                physical_id = vnode.physical_node_id

                if physical_id not in seen_physical:
                    seen_physical.add(physical_id)
                    result.append(physical_id)

                    if len(result) >= n:
                        break

            return result

    def get_ring_state(self) -> List[dict]:
        """
        Return the full ring state for visualization.
        Returns list of {position, physical_node, vnode_index} sorted by position.
        """
        with self._lock:
            state = []
            for pos in self._ring_positions:
                vnode = self._position_to_vnode[pos]
                state.append({
                    'position': pos,
                    'position_normalized': pos / self.RING_SIZE,  # 0.0 to 1.0
                    'physical_node': vnode.physical_node_id,
                    'vnode_index': vnode.vnode_index,
                })
            return state

    def get_key_position(self, key: str) -> float:
        """Get the normalized position (0.0 to 1.0) of a key on the ring."""
        hash_val = murmurhash3(key) % self.RING_SIZE
        return hash_val / self.RING_SIZE
