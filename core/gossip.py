"""
Gossip Protocol and Phi Accrual Failure Detector.

Implements epidemic-style state dissemination for cluster membership.
Each node periodically exchanges versioned state with a random peer.
The Phi Accrual Failure Detector provides a continuous suspicion level
based on historical heartbeat intervals, rather than a binary up/down.
"""

import random
import time
import math
import threading
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict
from enum import Enum


class NodeState(Enum):
    ALIVE = "alive"
    SUSPECT = "suspect"
    DEAD = "dead"
    JOINING = "joining"
    LEAVING = "leaving"


class PhiAccrualFailureDetector:
    """
    Phi Accrual Failure Detector.

    Instead of a binary alive/dead decision, this detector outputs a
    continuous suspicion level (phi). Higher phi means higher probability
    that the node has failed.

    The phi value is computed from the distribution of heartbeat inter-arrival
    times. If the current gap is much larger than the historical average,
    phi increases exponentially.
    """

    def __init__(self, threshold: float = 8.0, max_sample_size: int = 200,
                 min_std_deviation_ms: float = 100.0):
        self.threshold = threshold
        self.max_sample_size = max_sample_size
        self.min_std_deviation_ms = min_std_deviation_ms

        # Map node_id -> list of inter-arrival times (ms)
        self._intervals: Dict[str, List[float]] = defaultdict(list)
        # Map node_id -> last heartbeat timestamp
        self._last_heartbeat: Dict[str, float] = {}

    def heartbeat(self, node_id: str):
        """Record a heartbeat from the given node."""
        now = time.time() * 1000  # milliseconds
        if node_id in self._last_heartbeat:
            interval = now - self._last_heartbeat[node_id]
            samples = self._intervals[node_id]
            samples.append(interval)
            # Sliding window
            if len(samples) > self.max_sample_size:
                samples.pop(0)
        self._last_heartbeat[node_id] = now

    def phi(self, node_id: str) -> float:
        """
        Compute the suspicion level (phi) for a node.

        phi = -log10(P_later(t_now - t_last))

        Where P_later is the probability that the next heartbeat will
        arrive later than the current time, assuming a normal distribution
        of inter-arrival times.

        Returns:
            float: Suspicion level. Values > threshold suggest failure.
                   Returns 0.0 if insufficient data.
        """
        if node_id not in self._last_heartbeat:
            return 0.0

        samples = self._intervals.get(node_id, [])
        if len(samples) < 3:
            return 0.0

        now = time.time() * 1000
        time_since_last = now - self._last_heartbeat[node_id]

        # Compute mean and std deviation of inter-arrival times
        mean = sum(samples) / len(samples)
        variance = sum((x - mean) ** 2 for x in samples) / len(samples)
        std_dev = max(math.sqrt(variance), self.min_std_deviation_ms)

        # Compute phi using the complementary CDF of normal distribution
        y = (time_since_last - mean) / std_dev
        # Approximate the cumulative normal distribution
        e = math.exp(-y * (1.5976 + 0.070566 * y * y))
        if time_since_last > mean:
            p_later = e / (1.0 + e)
        else:
            p_later = 1.0 - 1.0 / (1.0 + e)

        # Avoid log(0)
        if p_later < 1e-15:
            p_later = 1e-15

        return -math.log10(p_later)

    def is_available(self, node_id: str) -> bool:
        """Check if a node is considered available (phi below threshold)."""
        return self.phi(node_id) < self.threshold

    def get_all_phi(self) -> Dict[str, float]:
        """Get phi values for all known nodes."""
        return {nid: self.phi(nid) for nid in self._last_heartbeat}


class GossipMember:
    """State information about a cluster member, disseminated via gossip."""

    def __init__(self, node_id: str, address: str, port: int):
        self.node_id = node_id
        self.address = address
        self.port = port
        self.state: NodeState = NodeState.JOINING
        self.heartbeat_counter: int = 0
        self.generation: int = int(time.time())
        self.metadata: Dict[str, str] = {}
        self.last_updated: float = time.time()

    def increment_heartbeat(self):
        self.heartbeat_counter += 1
        self.last_updated = time.time()

    def to_dict(self) -> dict:
        return {
            'node_id': self.node_id,
            'address': self.address,
            'port': self.port,
            'state': self.state.value,
            'heartbeat_counter': self.heartbeat_counter,
            'generation': self.generation,
            'metadata': self.metadata,
            'last_updated': self.last_updated
        }

    def is_newer_than(self, other: 'GossipMember') -> bool:
        """Check if this member info is newer than another."""
        if self.generation > other.generation:
            return True
        if self.generation == other.generation:
            return self.heartbeat_counter > other.heartbeat_counter
        return False


class GossipProtocol:
    """
    Epidemic Gossip Protocol for cluster membership and state dissemination.

    Every gossip interval, each node:
    1. Selects a random peer
    2. Sends its full membership state
    3. Merges the received state with its own

    This ensures eventual consistency of cluster membership views
    with O(log N) convergence time.
    """

    def __init__(self, local_node_id: str, address: str = "127.0.0.1",
                 port: int = 7000, gossip_interval: float = 1.0,
                 phi_threshold: float = 8.0):
        self.local_node_id = local_node_id
        self.gossip_interval = gossip_interval
        self._lock = threading.RLock()

        # Membership table
        self._members: Dict[str, GossipMember] = {}
        self._failure_detector = PhiAccrualFailureDetector(threshold=phi_threshold)

        # Gossip statistics
        self._gossip_rounds: int = 0
        self._messages_sent: int = 0
        self._messages_received: int = 0
        self._state_changes: List[dict] = []

        # Register self
        local_member = GossipMember(local_node_id, address, port)
        local_member.state = NodeState.ALIVE
        self._members[local_node_id] = local_member

    @property
    def members(self) -> Dict[str, GossipMember]:
        with self._lock:
            return dict(self._members)

    @property
    def alive_members(self) -> List[str]:
        """Return IDs of members currently considered alive."""
        with self._lock:
            result = []
            for nid, member in self._members.items():
                if member.state in (NodeState.ALIVE, NodeState.JOINING):
                    result.append(nid)
            return result

    def add_seed(self, node_id: str, address: str, port: int):
        """Add a seed node to bootstrap gossip."""
        with self._lock:
            if node_id not in self._members:
                member = GossipMember(node_id, address, port)
                member.state = NodeState.ALIVE
                self._members[node_id] = member
                self._failure_detector.heartbeat(node_id)

    def heartbeat(self):
        """
        Perform a single gossip round:
        1. Increment local heartbeat
        2. Select random peer
        3. Exchange state
        4. Update failure detection
        """
        with self._lock:
            # 1. Increment local heartbeat
            local = self._members[self.local_node_id]
            local.increment_heartbeat()
            self._failure_detector.heartbeat(self.local_node_id)
            self._gossip_rounds += 1

            # 2. Select random peer
            peers = [nid for nid in self._members if nid != self.local_node_id]
            if not peers:
                return

            target = random.choice(peers)
            self._messages_sent += 1

            # 3. Simulate state exchange (in a real system, this is a network call)
            # For simulation, we directly merge
            self._failure_detector.heartbeat(target)

            # 4. Update node states based on phi
            for nid in list(self._members.keys()):
                if nid == self.local_node_id:
                    continue
                phi = self._failure_detector.phi(nid)
                member = self._members[nid]
                old_state = member.state

                if phi > self._failure_detector.threshold * 2:
                    member.state = NodeState.DEAD
                elif phi > self._failure_detector.threshold:
                    member.state = NodeState.SUSPECT
                elif member.state in (NodeState.SUSPECT, NodeState.DEAD):
                    member.state = NodeState.ALIVE

                if old_state != member.state:
                    self._state_changes.append({
                        'time': time.time(),
                        'node_id': nid,
                        'from': old_state.value,
                        'to': member.state.value,
                        'phi': phi
                    })

    def merge_state(self, remote_members: Dict[str, dict]):
        """
        Merge received gossip state with local state.
        For each member, keep the version with the higher heartbeat counter.
        """
        with self._lock:
            self._messages_received += 1
            for nid, remote_data in remote_members.items():
                if nid not in self._members:
                    # New member discovered!
                    member = GossipMember(
                        nid,
                        remote_data.get('address', '127.0.0.1'),
                        remote_data.get('port', 7000)
                    )
                    member.state = NodeState(remote_data.get('state', 'alive'))
                    member.heartbeat_counter = remote_data.get('heartbeat_counter', 0)
                    member.generation = remote_data.get('generation', 0)
                    self._members[nid] = member
                    self._failure_detector.heartbeat(nid)
                else:
                    local = self._members[nid]
                    remote_gen = remote_data.get('generation', 0)
                    remote_hb = remote_data.get('heartbeat_counter', 0)

                    if (remote_gen > local.generation or
                        (remote_gen == local.generation and remote_hb > local.heartbeat_counter)):
                        local.heartbeat_counter = remote_hb
                        local.generation = remote_gen
                        local.last_updated = time.time()
                        self._failure_detector.heartbeat(nid)

    def mark_node_down(self, node_id: str):
        """Manually mark a node as dead (e.g., admin action)."""
        with self._lock:
            if node_id in self._members:
                self._members[node_id].state = NodeState.DEAD
                self._state_changes.append({
                    'time': time.time(),
                    'node_id': node_id,
                    'from': 'alive',
                    'to': 'dead',
                    'phi': 999.0
                })

    def mark_node_up(self, node_id: str):
        """Revive a node (e.g., after recovery)."""
        with self._lock:
            if node_id in self._members:
                member = self._members[node_id]
                member.state = NodeState.ALIVE
                member.generation = int(time.time())
                member.heartbeat_counter = 0
                self._failure_detector.heartbeat(node_id)
                self._state_changes.append({
                    'time': time.time(),
                    'node_id': node_id,
                    'from': 'dead',
                    'to': 'alive',
                    'phi': 0.0
                })

    def get_stats(self) -> dict:
        return {
            'gossip_rounds': self._gossip_rounds,
            'messages_sent': self._messages_sent,
            'messages_received': self._messages_received,
            'total_members': len(self._members),
            'alive_members': len(self.alive_members),
            'phi_values': self._failure_detector.get_all_phi(),
            'state_changes': self._state_changes[-50:],  # Last 50 events
        }
