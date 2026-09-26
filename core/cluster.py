"""
Vault Cluster - The Distributed Object Storage System.

This module ties together all core components into a fully functional
distributed storage cluster simulation:
- Consistent Hash Ring with vnodes for data partitioning
- Gossip Protocol for membership and failure detection
- Vector Clocks for conflict resolution
- Tunable Quorums (N, R, W) for consistency policies
- Hinted Handoff for transient failure recovery
- Anti-Entropy via Merkle Trees for long-term consistency
- Read Repair for opportunistic healing
- Background Scrubbing for proactive corruption detection
"""

import time
import random
import threading
import hashlib
import zlib
from typing import Dict, List, Optional, Tuple, Any, Set
from collections import defaultdict

from .hash_ring import ConsistentHashRing
from .gossip import GossipProtocol, NodeState
from .vector_clock import VectorClock, compare_versions
from .storage_engine import LocalStorageEngine, StoredObject
from .merkle_tree import MerkleTree


class QuorumConfig:
    """Tunable consistency configuration."""

    def __init__(self, n: int = 3, r: int = 2, w: int = 2):
        self.n = n  # Replication factor
        self.r = r  # Read quorum
        self.w = w  # Write quorum

    @property
    def is_strict(self) -> bool:
        """R + W > N guarantees strong consistency."""
        return self.r + self.w > self.n

    def to_dict(self) -> dict:
        return {
            'n': self.n, 'r': self.r, 'w': self.w,
            'is_strict': self.is_strict,
            'consistency_level': 'strong' if self.is_strict else 'eventual'
        }


class WriteResult:
    """Result of a distributed write operation."""

    def __init__(self):
        self.success: bool = False
        self.key: str = ""
        self.acks: int = 0
        self.required_acks: int = 0
        self.coordinator: str = ""
        self.replica_nodes: List[str] = []
        self.vector_clock: Optional[VectorClock] = None
        self.hinted_handoffs: List[str] = []
        self.duration_ms: float = 0
        self.error: str = ""

    def to_dict(self) -> dict:
        return {
            'success': self.success,
            'key': self.key,
            'acks': self.acks,
            'required_acks': self.required_acks,
            'coordinator': self.coordinator,
            'replica_nodes': self.replica_nodes,
            'vector_clock': self.vector_clock.to_dict() if self.vector_clock else None,
            'hinted_handoffs': self.hinted_handoffs,
            'duration_ms': round(self.duration_ms, 2),
            'error': self.error,
        }


class ReadResult:
    """Result of a distributed read operation."""

    def __init__(self):
        self.success: bool = False
        self.key: str = ""
        self.value: Optional[bytes] = None
        self.value_str: Optional[str] = None
        self.responses: int = 0
        self.required_responses: int = 0
        self.coordinator: str = ""
        self.vector_clock: Optional[VectorClock] = None
        self.read_repair_triggered: bool = False
        self.repaired_nodes: List[str] = []
        self.conflicts: List[str] = []
        self.duration_ms: float = 0
        self.error: str = ""
        self.responding_nodes: List[str] = []

    def to_dict(self) -> dict:
        return {
            'success': self.success,
            'key': self.key,
            'value': self.value_str,
            'size': len(self.value) if self.value else 0,
            'responses': self.responses,
            'required_responses': self.required_responses,
            'coordinator': self.coordinator,
            'vector_clock': self.vector_clock.to_dict() if self.vector_clock else None,
            'read_repair_triggered': self.read_repair_triggered,
            'repaired_nodes': self.repaired_nodes,
            'conflicts': self.conflicts,
            'duration_ms': round(self.duration_ms, 2),
            'error': self.error,
            'responding_nodes': self.responding_nodes,
        }


class VaultCluster:
    """
    The main Vault distributed object storage cluster.

    Coordinates all distributed operations including:
    - Object placement via consistent hashing
    - Quorum-based reads and writes
    - Automatic hinted handoff
    - Read repair
    - Anti-entropy synchronization
    - Background scrubbing
    """

    def __init__(self, vnodes_per_node: int = 32, quorum: QuorumConfig = None):
        self._lock = threading.RLock()

        # Core components
        self.hash_ring = ConsistentHashRing(vnodes_per_node=vnodes_per_node)
        self.quorum = quorum or QuorumConfig(n=3, r=2, w=2)

        # Per-node components
        self._gossip_protocols: Dict[str, GossipProtocol] = {}
        self._storage_engines: Dict[str, LocalStorageEngine] = {}
        self._node_states: Dict[str, str] = {}  # node_id -> state

        # Cluster-wide event log for visualization
        self._event_log: List[dict] = []
        self._max_events = 500

        # Rebalancing state
        self._rebalancing = False
        self._rebalance_progress: Dict[str, float] = {}

        # Metrics
        self._metrics = {
            'total_writes': 0,
            'total_reads': 0,
            'successful_writes': 0,
            'successful_reads': 0,
            'failed_writes': 0,
            'failed_reads': 0,
            'total_hinted_handoffs': 0,
            'total_read_repairs': 0,
            'total_anti_entropy_syncs': 0,
            'total_scrubs': 0,
            'total_corruptions_detected': 0,
        }

    def _log_event(self, event_type: str, details: dict):
        """Log an event for the dashboard visualization."""
        event = {
            'timestamp': time.time(),
            'type': event_type,
            **details
        }
        self._event_log.append(event)
        if len(self._event_log) > self._max_events:
            self._event_log = self._event_log[-self._max_events:]

    # --- Cluster Topology ---

    def add_node(self, node_id: str, address: str = "127.0.0.1",
                 port: int = 7000) -> dict:
        """
        Add a new node to the Vault cluster.

        This involves:
        1. Adding vnodes to the hash ring
        2. Starting gossip protocol for the node
        3. Creating local storage engine
        4. Notifying all existing nodes via gossip
        """
        with self._lock:
            # Add to hash ring
            vnodes = self.hash_ring.add_node(node_id)

            # Create gossip protocol instance
            gossip = GossipProtocol(node_id, address, port)
            self._gossip_protocols[node_id] = gossip

            # Register all existing nodes as seeds
            for existing_id, existing_gossip in self._gossip_protocols.items():
                if existing_id != node_id:
                    gossip.add_seed(existing_id, address, port + hash(existing_id) % 1000)
                    existing_gossip.add_seed(node_id, address, port)

            # Create storage engine
            self._storage_engines[node_id] = LocalStorageEngine(node_id)
            self._node_states[node_id] = NodeState.ALIVE.value

            self._log_event('node_added', {
                'node_id': node_id,
                'vnodes_count': len(vnodes),
                'total_nodes': len(self.hash_ring.nodes),
            })

            return {
                'node_id': node_id,
                'vnodes_added': len(vnodes),
                'total_nodes': len(self.hash_ring.nodes),
                'total_vnodes': self.hash_ring.total_vnodes,
            }

    def remove_node(self, node_id: str) -> dict:
        """Remove a node from the cluster."""
        with self._lock:
            vnodes = self.hash_ring.remove_node(node_id)

            if node_id in self._gossip_protocols:
                for gid, gossip in self._gossip_protocols.items():
                    if gid != node_id:
                        gossip.mark_node_down(node_id)

            self._node_states[node_id] = NodeState.DEAD.value

            self._log_event('node_removed', {
                'node_id': node_id,
                'vnodes_removed': len(vnodes),
                'remaining_nodes': len(self.hash_ring.nodes),
            })

            return {
                'node_id': node_id,
                'vnodes_removed': len(vnodes),
                'remaining_nodes': len(self.hash_ring.nodes),
            }

    def kill_node(self, node_id: str) -> dict:
        """
        Simulate a node crash (does NOT remove from ring, just makes unavailable).
        Data remains in storage but node stops responding to reads/writes.
        """
        with self._lock:
            if node_id in self._node_states:
                self._node_states[node_id] = NodeState.DEAD.value

                for gid, gossip in self._gossip_protocols.items():
                    if gid != node_id:
                        gossip.mark_node_down(node_id)

                self._log_event('node_killed', {
                    'node_id': node_id,
                    'message': f'Node {node_id} crashed!'
                })

                return {'node_id': node_id, 'state': 'dead', 'message': 'Node killed'}
            return {'error': f'Node {node_id} not found'}

    def revive_node(self, node_id: str) -> dict:
        """Bring a crashed node back online."""
        with self._lock:
            if node_id in self._node_states:
                self._node_states[node_id] = NodeState.ALIVE.value

                for gid, gossip in self._gossip_protocols.items():
                    gossip.mark_node_up(node_id)

                # Deliver any hinted handoffs
                hints_delivered = self._deliver_hints(node_id)

                self._log_event('node_revived', {
                    'node_id': node_id,
                    'hints_delivered': hints_delivered,
                })

                return {
                    'node_id': node_id,
                    'state': 'alive',
                    'hints_delivered': hints_delivered,
                }
            return {'error': f'Node {node_id} not found'}

    def _is_node_available(self, node_id: str) -> bool:
        """Check if a node is available for operations."""
        return self._node_states.get(node_id) == NodeState.ALIVE.value

    # --- Write Path (Coordinator Logic) ---

    def write(self, key: str, value: str, client_vc: Optional[dict] = None) -> WriteResult:
        """
        Distributed write operation.

        1. Hash the key to find the coordinator
        2. Determine the N replica nodes via consistent hashing
        3. Write to all available replicas
        4. Wait for W acknowledgments
        5. If a replica is unavailable, store a hinted handoff
        """
        start = time.time()
        result = WriteResult()
        result.key = key
        result.required_acks = self.quorum.w

        with self._lock:
            self._metrics['total_writes'] += 1

            # Find coordinator and replica set
            replica_nodes = self.hash_ring.get_replica_nodes(key, self.quorum.n)
            if not replica_nodes:
                result.error = "No nodes available in the cluster"
                result.duration_ms = (time.time() - start) * 1000
                self._metrics['failed_writes'] += 1
                return result

            coordinator = replica_nodes[0]
            result.coordinator = coordinator
            result.replica_nodes = replica_nodes

            # Build vector clock
            if client_vc:
                vc = VectorClock.from_dict(client_vc)
            else:
                # Get existing version if any
                existing_vc = None
                for node_id in replica_nodes:
                    if self._is_node_available(node_id) and node_id in self._storage_engines:
                        existing_vc = self._storage_engines[node_id].get_version(key)
                        if existing_vc:
                            break
                vc = existing_vc or VectorClock()

            # Increment the coordinator's counter
            vc = vc.increment(coordinator)
            result.vector_clock = vc

            value_bytes = value.encode('utf-8')
            acks = 0
            hinted = []

            # Write to each replica
            for node_id in replica_nodes:
                if self._is_node_available(node_id) and node_id in self._storage_engines:
                    try:
                        self._storage_engines[node_id].put(key, value_bytes, vc, coordinator)
                        acks += 1
                    except Exception as e:
                        self._log_event('write_error', {
                            'node_id': node_id, 'key': key, 'error': str(e)
                        })
                else:
                    # Hinted handoff: store on coordinator for later delivery
                    if coordinator in self._storage_engines and self._is_node_available(coordinator):
                        self._storage_engines[coordinator].store_hint(
                            node_id, key, value_bytes, vc, coordinator
                        )
                        hinted.append(node_id)
                        self._metrics['total_hinted_handoffs'] += 1
                        self._log_event('hinted_handoff', {
                            'key': key,
                            'from': coordinator,
                            'target': node_id,
                        })
                    else:
                        # Try another available node for the hint
                        for alt_node in self.hash_ring.nodes:
                            if (self._is_node_available(alt_node) and
                                alt_node in self._storage_engines and
                                alt_node != node_id):
                                self._storage_engines[alt_node].store_hint(
                                    node_id, key, value_bytes, vc, coordinator
                                )
                                hinted.append(node_id)
                                self._metrics['total_hinted_handoffs'] += 1
                                break

            result.acks = acks
            result.hinted_handoffs = hinted
            result.success = acks >= self.quorum.w

            if result.success:
                self._metrics['successful_writes'] += 1
                self._log_event('write_success', {
                    'key': key, 'acks': acks, 'coordinator': coordinator,
                    'replicas': replica_nodes, 'hinted': hinted,
                    'size': len(value_bytes),
                })
            else:
                self._metrics['failed_writes'] += 1
                result.error = f"Only {acks}/{self.quorum.w} acks received"
                self._log_event('write_failure', {
                    'key': key, 'acks': acks, 'required': self.quorum.w,
                })

            result.duration_ms = (time.time() - start) * 1000
            return result

    # --- Read Path (Coordinator Logic) ---

    def read(self, key: str) -> ReadResult:
        """
        Distributed read operation.

        1. Hash the key to find replica nodes
        2. Send read request to N replicas
        3. Wait for R responses
        4. Compare vector clocks to find the latest version
        5. Trigger read repair if stale replicas detected
        """
        start = time.time()
        result = ReadResult()
        result.key = key

        with self._lock:
            self._metrics['total_reads'] += 1

            replica_nodes = self.hash_ring.get_replica_nodes(key, self.quorum.n)
            if not replica_nodes:
                result.error = "No nodes available"
                result.duration_ms = (time.time() - start) * 1000
                self._metrics['failed_reads'] += 1
                return result

            result.coordinator = replica_nodes[0]
            result.required_responses = self.quorum.r

            # Collect responses from available replicas
            responses: List[Tuple[str, VectorClock, bytes]] = []

            for node_id in replica_nodes:
                if self._is_node_available(node_id) and node_id in self._storage_engines:
                    try:
                        obj = self._storage_engines[node_id].get(key)
                        if obj:
                            responses.append((node_id, obj.vector_clock, obj.value))
                            result.responding_nodes.append(node_id)
                    except ValueError as e:
                        # Corruption detected — try to repair from another replica
                        self._log_event('corruption_detected', {
                            'key': key, 'node_id': node_id, 'error': str(e)
                        })
                        self._metrics['total_corruptions_detected'] += 1

            result.responses = len(responses)

            if len(responses) < self.quorum.r:
                result.error = f"Only {len(responses)}/{self.quorum.r} responses"
                result.duration_ms = (time.time() - start) * 1000
                self._metrics['failed_reads'] += 1

                # If we have ANY response, return it with a warning
                if responses:
                    _, vc, val = responses[0]
                    result.value = val
                    result.value_str = val.decode('utf-8', errors='replace')
                    result.vector_clock = vc
                    result.success = True  # Partial success
                return result

            # Conflict resolution via vector clocks
            winner, conflicts = compare_versions(responses)

            if winner is not None:
                # Clear winner
                result.value = winner
                result.value_str = winner.decode('utf-8', errors='replace')
                result.success = True

                # Find the winning vector clock
                for node_id, vc, val in responses:
                    if val == winner:
                        result.vector_clock = vc
                        break
            elif conflicts:
                # Concurrent versions detected — use LWW as fallback
                result.conflicts = [v.decode('utf-8', errors='replace') for v in conflicts]

                # LWW fallback: pick the one with the latest timestamp
                latest = max(responses, key=lambda r: r[1].timestamp)
                result.value = latest[2]
                result.value_str = latest[2].decode('utf-8', errors='replace')
                result.vector_clock = latest[1]
                result.success = True

                self._log_event('conflict_resolved', {
                    'key': key, 'num_conflicts': len(conflicts),
                    'resolution': 'LWW',
                })
            else:
                result.error = "No data found"
                self._metrics['failed_reads'] += 1
                result.duration_ms = (time.time() - start) * 1000
                return result

            # --- Read Repair ---
            if result.success and result.vector_clock:
                repaired = self._read_repair(key, result.value, result.vector_clock, replica_nodes)
                if repaired:
                    result.read_repair_triggered = True
                    result.repaired_nodes = repaired
                    self._metrics['total_read_repairs'] += len(repaired)

            self._metrics['successful_reads'] += 1
            self._log_event('read_success', {
                'key': key, 'responses': len(responses),
                'coordinator': result.coordinator,
                'read_repair': result.read_repair_triggered,
            })

            result.duration_ms = (time.time() - start) * 1000
            return result

    def _read_repair(self, key: str, latest_value: bytes,
                     latest_vc: VectorClock,
                     replica_nodes: List[str]) -> List[str]:
        """
        Read Repair: if any replica has stale or missing data,
        asynchronously update it with the latest version.
        """
        repaired = []
        for node_id in replica_nodes:
            if not self._is_node_available(node_id) or node_id not in self._storage_engines:
                continue

            engine = self._storage_engines[node_id]
            existing_vc = engine.get_version(key)

            needs_repair = False
            if existing_vc is None:
                needs_repair = True
            elif latest_vc.dominates(existing_vc):
                needs_repair = True

            if needs_repair:
                engine.put(key, latest_value, latest_vc)
                engine._stats['read_repairs'] += 1
                repaired.append(node_id)
                self._log_event('read_repair', {
                    'key': key, 'node_id': node_id,
                })

        return repaired

    # --- Hinted Handoff Delivery ---

    def _deliver_hints(self, target_node: str) -> int:
        """Deliver all hinted handoffs destined for the target node."""
        delivered = 0
        if not self._is_node_available(target_node):
            return 0

        target_engine = self._storage_engines.get(target_node)
        if not target_engine:
            return 0

        # Check all nodes for hints targeted at this node
        for node_id, engine in self._storage_engines.items():
            hints = engine.get_hints_for(target_node)
            for hint in hints:
                target_engine.put(hint.key, hint.value, hint.vector_clock, hint.coordinator_node)
                delivered += 1
                self._log_event('hint_delivered', {
                    'key': hint.key,
                    'from': node_id,
                    'to': target_node,
                })

        return delivered

    # --- Anti-Entropy Synchronization ---

    def run_anti_entropy(self) -> dict:
        """
        Run anti-entropy synchronization between all replica pairs.

        For each key range, compare Merkle tree roots between replicas.
        If they differ, traverse the tree to find and repair inconsistencies.
        """
        with self._lock:
            self._metrics['total_anti_entropy_syncs'] += 1
            synced = 0
            repairs = 0
            pairs_checked = 0

            alive_nodes = [n for n in self.hash_ring.nodes if self._is_node_available(n)]

            for i, node_a in enumerate(alive_nodes):
                for node_b in alive_nodes[i+1:]:
                    engine_a = self._storage_engines.get(node_a)
                    engine_b = self._storage_engines.get(node_b)

                    if not engine_a or not engine_b:
                        continue

                    pairs_checked += 1

                    # Compare Merkle roots
                    root_a = engine_a.get_merkle_root()
                    root_b = engine_b.get_merkle_root()

                    if root_a == root_b:
                        synced += 1
                        continue

                    # Trees differ — find differing keys
                    diff_keys = engine_a.compare_with(engine_b.get_merkle_tree())

                    for key in diff_keys:
                        # Determine which version is newer
                        vc_a = engine_a.get_version(key)
                        vc_b = engine_b.get_version(key)

                        if vc_a and vc_b:
                            if vc_a.dominates(vc_b):
                                obj = engine_a.get(key)
                                if obj:
                                    engine_b.put(key, obj.value, obj.vector_clock, obj.coordinator_node)
                                    repairs += 1
                            elif vc_b.dominates(vc_a):
                                obj = engine_b.get(key)
                                if obj:
                                    engine_a.put(key, obj.value, obj.vector_clock, obj.coordinator_node)
                                    repairs += 1
                        elif vc_a and not vc_b:
                            # B is missing the object
                            obj = engine_a.get(key)
                            if obj:
                                # Only replicate if node_b should have it
                                replicas = self.hash_ring.get_replica_nodes(key, self.quorum.n)
                                if node_b in replicas:
                                    engine_b.put(key, obj.value, obj.vector_clock, obj.coordinator_node)
                                    repairs += 1
                        elif vc_b and not vc_a:
                            obj = engine_b.get(key)
                            if obj:
                                replicas = self.hash_ring.get_replica_nodes(key, self.quorum.n)
                                if node_a in replicas:
                                    engine_a.put(key, obj.value, obj.vector_clock, obj.coordinator_node)
                                    repairs += 1

            result = {
                'pairs_checked': pairs_checked,
                'in_sync': synced,
                'repairs_made': repairs,
            }

            if repairs > 0:
                self._log_event('anti_entropy', result)

            return result

    # --- Background Scrubbing ---

    def run_scrub(self) -> dict:
        """Run integrity scrub on all nodes."""
        with self._lock:
            self._metrics['total_scrubs'] += 1
            total_corrupted = []

            for node_id, engine in self._storage_engines.items():
                if not self._is_node_available(node_id):
                    continue
                corrupted = engine.scrub()
                if corrupted:
                    total_corrupted.extend(
                        [{'key': k, 'node': node_id} for k in corrupted]
                    )
                    self._metrics['total_corruptions_detected'] += len(corrupted)

                    # Auto-repair from other replicas
                    for key in corrupted:
                        replicas = self.hash_ring.get_replica_nodes(key, self.quorum.n)
                        for replica_id in replicas:
                            if (replica_id != node_id and
                                self._is_node_available(replica_id) and
                                replica_id in self._storage_engines):
                                try:
                                    healthy_obj = self._storage_engines[replica_id].get(key)
                                    if healthy_obj:
                                        engine.put(key, healthy_obj.value,
                                                   healthy_obj.vector_clock,
                                                   healthy_obj.coordinator_node)
                                        self._log_event('auto_repair', {
                                            'key': key,
                                            'corrupted_node': node_id,
                                            'repaired_from': replica_id,
                                        })
                                        break
                                except ValueError:
                                    continue

            result = {
                'nodes_scrubbed': len([n for n in self._storage_engines
                                       if self._is_node_available(n)]),
                'corruptions_found': len(total_corrupted),
                'corrupted_objects': total_corrupted,
            }

            if total_corrupted:
                self._log_event('scrub_complete', result)

            return result

    # --- Chaos Operations ---

    def corrupt_random_object(self) -> dict:
        """Simulate random data corruption on a random node."""
        with self._lock:
            available = [n for n in self._storage_engines
                         if self._is_node_available(n) and self._storage_engines[n].keys()]
            if not available:
                return {'error': 'No data to corrupt'}

            node_id = random.choice(available)
            engine = self._storage_engines[node_id]
            keys = engine.keys()
            key = random.choice(keys)

            if engine.corrupt_object(key):
                self._log_event('corruption_injected', {
                    'key': key, 'node_id': node_id,
                })
                return {'key': key, 'node_id': node_id, 'corrupted': True}
            return {'error': 'Failed to corrupt'}

    # --- Gossip Simulation ---

    def run_gossip_round(self):
        """Run one gossip round on all nodes."""
        with self._lock:
            for node_id, gossip in self._gossip_protocols.items():
                if self._is_node_available(node_id):
                    gossip.heartbeat()

    # --- Dashboard Data ---

    def get_cluster_state(self) -> dict:
        """Get full cluster state for the dashboard."""
        with self._lock:
            nodes = []
            for node_id in self._storage_engines:
                engine = self._storage_engines[node_id]
                gossip = self._gossip_protocols.get(node_id)

                node_info = {
                    'node_id': node_id,
                    'state': self._node_states.get(node_id, 'unknown'),
                    'storage': engine.get_stats(),
                    'gossip': gossip.get_stats() if gossip else {},
                }
                nodes.append(node_info)

            # Get ring visualization with reduced vnodes
            ring_state = self.hash_ring.get_ring_state()
            # Sample vnodes for visualization (max 200 points)
            if len(ring_state) > 200:
                step = len(ring_state) // 200
                ring_viz = ring_state[::step]
            else:
                ring_viz = ring_state

            return {
                'nodes': nodes,
                'ring': ring_viz,
                'quorum': self.quorum.to_dict(),
                'metrics': dict(self._metrics),
                'events': self._event_log[-100:],
                'total_nodes': len(self._storage_engines),
                'alive_nodes': len([n for n in self._node_states
                                    if self._node_states[n] == NodeState.ALIVE.value]),
                'dead_nodes': len([n for n in self._node_states
                                   if self._node_states[n] == NodeState.DEAD.value]),
            }

    def get_object_distribution(self) -> dict:
        """Get object distribution across nodes for visualization."""
        with self._lock:
            distribution = {}
            for node_id, engine in self._storage_engines.items():
                distribution[node_id] = {
                    'count': len(engine.keys()),
                    'bytes': engine.get_stats()['bytes_stored'],
                    'state': self._node_states.get(node_id, 'unknown'),
                }
            return distribution

    def get_object_replicas(self, key: str) -> dict:
        """Get replica information for a specific object."""
        with self._lock:
            replicas = self.hash_ring.get_replica_nodes(key, self.quorum.n)
            position = self.hash_ring.get_key_position(key)

            replica_info = []
            for node_id in replicas:
                engine = self._storage_engines.get(node_id)
                has_data = engine.has_key(key) if engine else False

                info = {
                    'node_id': node_id,
                    'state': self._node_states.get(node_id, 'unknown'),
                    'has_data': has_data,
                }

                if has_data and engine:
                    try:
                        obj = engine.get(key)
                        if obj:
                            info['version'] = obj.vector_clock.to_dict()
                            info['size'] = obj.size
                            info['checksum'] = hex(obj.checksum)
                            info['integrity'] = obj.verify_integrity()
                    except ValueError:
                        info['integrity'] = False

                replica_info.append(info)

            return {
                'key': key,
                'position': position,
                'replicas': replica_info,
                'replication_factor': self.quorum.n,
            }
