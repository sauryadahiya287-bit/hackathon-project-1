"""
Vector Clocks for Causality Tracking and Conflict Resolution.

Each object version is tagged with a vector clock — a map of
{node_id: logical_counter}. By comparing vector clocks, we can determine:
  - One version strictly dominates another (can safely discard the old).
  - Two versions are concurrent (requires conflict resolution).
"""

from typing import Dict, List, Tuple, Optional
import copy
import time


class VectorClock:
    """
    A Vector Clock tracks causal ordering of events across distributed nodes.

    Each node maintains its own counter. When a node performs a write, it
    increments its counter. When nodes exchange data, they merge their clocks
    by taking the max of each entry.
    """

    def __init__(self, clock: Optional[Dict[str, int]] = None):
        self.clock: Dict[str, int] = clock or {}
        self.timestamp: float = time.time()

    def increment(self, node_id: str) -> 'VectorClock':
        """
        Increment the counter for the given node.
        Returns a NEW VectorClock (immutable semantics for safety).
        """
        new_clock = VectorClock(copy.deepcopy(self.clock))
        new_clock.clock[node_id] = new_clock.clock.get(node_id, 0) + 1
        new_clock.timestamp = time.time()
        return new_clock

    def merge(self, other: 'VectorClock') -> 'VectorClock':
        """
        Merge two vector clocks by taking the component-wise maximum.
        Returns a new VectorClock.
        """
        merged = {}
        all_nodes = set(self.clock.keys()) | set(other.clock.keys())
        for node in all_nodes:
            merged[node] = max(
                self.clock.get(node, 0),
                other.clock.get(node, 0)
            )
        result = VectorClock(merged)
        result.timestamp = max(self.timestamp, other.timestamp)
        return result

    def dominates(self, other: 'VectorClock') -> bool:
        """
        Check if this clock strictly dominates (happens-after) the other.

        self dominates other iff:
          - For all nodes in other.clock: self[node] >= other[node]
          - There exists at least one node where self[node] > other[node]
        """
        at_least_one_greater = False
        for node, count in other.clock.items():
            my_count = self.clock.get(node, 0)
            if my_count < count:
                return False
            if my_count > count:
                at_least_one_greater = True

        # Also check for nodes in self but not in other
        for node, count in self.clock.items():
            if node not in other.clock and count > 0:
                at_least_one_greater = True

        return at_least_one_greater

    def is_concurrent(self, other: 'VectorClock') -> bool:
        """
        Two clocks are concurrent if neither dominates the other
        and they are not equal.
        """
        if self == other:
            return False
        return not self.dominates(other) and not other.dominates(self)

    def __eq__(self, other):
        if not isinstance(other, VectorClock):
            return False
        # Two clocks are equal if all entries match
        all_nodes = set(self.clock.keys()) | set(other.clock.keys())
        for node in all_nodes:
            if self.clock.get(node, 0) != other.clock.get(node, 0):
                return False
        return True

    def __repr__(self):
        entries = ', '.join(f'{n}:{c}' for n, c in sorted(self.clock.items()))
        return f'VC({entries})'

    def to_dict(self) -> dict:
        return {
            'clock': dict(self.clock),
            'timestamp': self.timestamp
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'VectorClock':
        vc = cls(data.get('clock', {}))
        vc.timestamp = data.get('timestamp', time.time())
        return vc


def compare_versions(versions: List[Tuple[str, 'VectorClock', any]]) -> Tuple[Optional[any], List[any]]:
    """
    Compare multiple versioned values and determine the winner(s).

    Args:
        versions: List of (node_id, vector_clock, value) tuples

    Returns:
        (winner, conflicts):
            - winner: The single winning value if one clock dominates, else None
            - conflicts: List of all conflicting values if concurrent
    """
    if not versions:
        return None, []

    if len(versions) == 1:
        return versions[0][2], []

    # Check if all vector clocks are equal (most common case: all replicas in sync)
    all_equal = True
    _, first_vc, _ = versions[0]
    for _, vc, _ in versions[1:]:
        if vc != first_vc:
            all_equal = False
            break

    if all_equal:
        return versions[0][2], []

    # Try to find a single dominator
    best_idx = 0

    for i in range(1, len(versions)):
        _, best_vc, _ = versions[best_idx]
        _, curr_vc, _ = versions[i]

        if curr_vc.dominates(best_vc):
            best_idx = i

    # Verify the "best" actually dominates all others
    _, best_vc, best_val = versions[best_idx]
    truly_dominates_all = True
    conflict_values = []

    for i, (_, vc, val) in enumerate(versions):
        if i == best_idx:
            continue
        if not best_vc.dominates(vc) and best_vc != vc:
            truly_dominates_all = False
            conflict_values.append(val)

    if truly_dominates_all:
        return best_val, []
    else:
        # All versions are in conflict — return them all
        conflict_values.insert(0, best_val)
        return None, conflict_values
