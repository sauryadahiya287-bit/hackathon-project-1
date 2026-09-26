"""
Merkle Tree for Anti-Entropy Synchronization.

Each node maintains a Merkle tree per vnode. The tree enables O(log n)
detection of divergent data blocks between replicas by comparing tree
root hashes and selectively traversing only the differing branches.
"""

import hashlib
import math
from typing import Dict, List, Optional, Set, Tuple


class MerkleNode:
    """A node in the Merkle tree."""
    __slots__ = ['hash_value', 'left', 'right', 'key', 'is_leaf']

    def __init__(self, hash_value: str = "", key: str = ""):
        self.hash_value = hash_value
        self.left: Optional['MerkleNode'] = None
        self.right: Optional['MerkleNode'] = None
        self.key = key
        self.is_leaf = False


class MerkleTree:
    """
    Merkle Tree (Hash Tree) for efficient replica comparison.

    Leaves represent individual object hashes. Interior nodes contain
    the hash of their children. If two replicas' root hashes match,
    they are perfectly synchronized. If they differ, we traverse down
    to find exactly which objects are inconsistent.

    The tree is a balanced binary tree for O(log n) comparison depth.
    """

    def __init__(self):
        self.root: Optional[MerkleNode] = None
        self._data: Dict[str, str] = {}  # key -> hash of value
        self._leaf_count: int = 0
        self._depth: int = 0

    def _hash(self, data: str) -> str:
        """Compute SHA-256 hash of data."""
        return hashlib.sha256(data.encode('utf-8')).hexdigest()

    def _combine_hashes(self, h1: str, h2: str) -> str:
        """Combine two hashes by concatenating and hashing again."""
        return self._hash(h1 + h2)

    def insert(self, key: str, value_hash: str):
        """Insert or update a key with its value hash."""
        self._data[key] = value_hash
        self._rebuild()

    def remove(self, key: str):
        """Remove a key from the tree."""
        if key in self._data:
            del self._data[key]
            self._rebuild()

    def _rebuild(self):
        """Rebuild the entire Merkle tree from the current data."""
        if not self._data:
            self.root = None
            self._leaf_count = 0
            self._depth = 0
            return

        # Sort keys for deterministic tree construction
        sorted_keys = sorted(self._data.keys())
        self._leaf_count = len(sorted_keys)

        # Create leaf nodes
        leaves = []
        for key in sorted_keys:
            node = MerkleNode(
                hash_value=self._data[key],
                key=key
            )
            node.is_leaf = True
            leaves.append(node)

        # Pad to power of 2 for balanced tree
        target_size = 1
        while target_size < len(leaves):
            target_size *= 2

        while len(leaves) < target_size:
            pad = MerkleNode(hash_value=self._hash("__empty__"), key="")
            pad.is_leaf = True
            leaves.append(pad)

        # Build tree bottom-up
        self._depth = int(math.log2(target_size)) if target_size > 0 else 0
        current_level = leaves

        while len(current_level) > 1:
            next_level = []
            for i in range(0, len(current_level), 2):
                left = current_level[i]
                right = current_level[i + 1] if i + 1 < len(current_level) else left

                parent = MerkleNode(
                    hash_value=self._combine_hashes(left.hash_value, right.hash_value)
                )
                parent.left = left
                parent.right = right
                next_level.append(parent)

            current_level = next_level

        self.root = current_level[0] if current_level else None

    def get_root_hash(self) -> str:
        """Get the root hash of the Merkle tree."""
        if self.root:
            return self.root.hash_value
        return ""

    def diff(self, other: 'MerkleTree') -> Set[str]:
        """
        Find keys that differ between this tree and another.

        Uses top-down comparison, traversing only branches that have
        different hashes. This minimizes the comparison work to O(d * log n)
        where d is the number of differences.
        """
        if not self.root or not other.root:
            # One or both are empty — all keys differ
            return set(self._data.keys()) | set(other._data.keys())

        if self.root.hash_value == other.root.hash_value:
            return set()  # Trees are identical

        # Trees differ — find which leaves differ
        differing_keys = set()
        self._diff_recursive(self.root, other.root, differing_keys)
        return differing_keys

    def _diff_recursive(self, node1: Optional[MerkleNode],
                        node2: Optional[MerkleNode],
                        result: Set[str]):
        """Recursively find differing leaves."""
        if node1 is None and node2 is None:
            return

        if node1 is None or node2 is None:
            # Collect all leaves from the non-None subtree
            self._collect_leaves(node1 or node2, result)
            return

        if node1.hash_value == node2.hash_value:
            return  # This entire subtree is identical

        if node1.is_leaf or node2.is_leaf:
            if node1.key:
                result.add(node1.key)
            if node2.key:
                result.add(node2.key)
            return

        # Recurse into children
        self._diff_recursive(node1.left, node2.left, result)
        self._diff_recursive(node1.right, node2.right, result)

    def _collect_leaves(self, node: Optional[MerkleNode], result: Set[str]):
        """Collect all leaf keys from a subtree."""
        if node is None:
            return
        if node.is_leaf:
            if node.key:
                result.add(node.key)
            return
        self._collect_leaves(node.left, result)
        self._collect_leaves(node.right, result)

    def get_stats(self) -> dict:
        return {
            'root_hash': self.get_root_hash()[:16] + '...' if self.root else 'empty',
            'leaf_count': self._leaf_count,
            'depth': self._depth,
            'total_keys': len(self._data),
        }

    def get_tree_visualization(self, max_depth: int = 4) -> List[dict]:
        """
        Return tree structure for visualization (limited depth).
        """
        if not self.root:
            return []

        result = []
        self._visualize_recursive(self.root, 0, max_depth, result)
        return result

    def _visualize_recursive(self, node: Optional[MerkleNode],
                             depth: int, max_depth: int,
                             result: List[dict]):
        if node is None or depth > max_depth:
            return

        result.append({
            'depth': depth,
            'hash': node.hash_value[:12],
            'key': node.key if node.is_leaf else '',
            'is_leaf': node.is_leaf,
            'has_children': node.left is not None or node.right is not None,
        })

        self._visualize_recursive(node.left, depth + 1, max_depth, result)
        self._visualize_recursive(node.right, depth + 1, max_depth, result)
