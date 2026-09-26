"""
Vault API Server

Flask-based HTTP API that exposes the Vault distributed storage cluster
for the web dashboard. Provides endpoints for:
- Cluster management (add/remove/kill/revive nodes)
- Data operations (read/write/delete)
- Background operations (gossip, anti-entropy, scrub)
- Real-time cluster state for visualization
"""

import os
import sys
import time
import threading
import json

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.cluster import VaultCluster, QuorumConfig

app = Flask(__name__, static_folder='dashboard', static_url_path='')
CORS(app)

# Global cluster instance
cluster = VaultCluster(vnodes_per_node=32, quorum=QuorumConfig(n=3, r=2, w=2))

# Background gossip thread
gossip_running = True
gossip_interval = 1.0


def gossip_worker():
    """Background thread that runs gossip rounds periodically."""
    while gossip_running:
        try:
            cluster.run_gossip_round()
        except Exception as e:
            pass
        time.sleep(gossip_interval)


# Start gossip worker
gossip_thread = threading.Thread(target=gossip_worker, daemon=True)
gossip_thread.start()


# --- Static Files (Dashboard) ---

@app.route('/')
def serve_dashboard():
    return send_from_directory('dashboard', 'index.html')


# --- Cluster Management ---

@app.route('/api/cluster/state', methods=['GET'])
def get_cluster_state():
    """Get full cluster state for dashboard visualization."""
    return jsonify(cluster.get_cluster_state())


@app.route('/api/cluster/init', methods=['POST'])
def init_cluster():
    """Initialize cluster with a set of nodes."""
    data = request.json or {}
    num_nodes = data.get('num_nodes', 5)
    node_prefix = data.get('prefix', 'vault-node')

    results = []
    for i in range(num_nodes):
        node_id = f"{node_prefix}-{i+1}"
        result = cluster.add_node(node_id, "127.0.0.1", 7000 + i)
        results.append(result)

    return jsonify({
        'nodes_created': len(results),
        'results': results,
    })


@app.route('/api/cluster/add-node', methods=['POST'])
def add_node():
    """Add a single node to the cluster."""
    data = request.json or {}
    node_id = data.get('node_id', f"vault-node-{int(time.time())}")
    result = cluster.add_node(node_id)
    return jsonify(result)


@app.route('/api/cluster/remove-node', methods=['POST'])
def remove_node():
    """Remove a node from the cluster."""
    data = request.json or {}
    node_id = data.get('node_id')
    if not node_id:
        return jsonify({'error': 'node_id required'}), 400
    result = cluster.remove_node(node_id)
    return jsonify(result)


@app.route('/api/cluster/kill-node', methods=['POST'])
def kill_node():
    """Simulate a node crash."""
    data = request.json or {}
    node_id = data.get('node_id')
    if not node_id:
        return jsonify({'error': 'node_id required'}), 400
    result = cluster.kill_node(node_id)
    return jsonify(result)


@app.route('/api/cluster/revive-node', methods=['POST'])
def revive_node():
    """Bring a crashed node back online."""
    data = request.json or {}
    node_id = data.get('node_id')
    if not node_id:
        return jsonify({'error': 'node_id required'}), 400
    result = cluster.revive_node(node_id)
    return jsonify(result)


# --- Quorum Configuration ---

@app.route('/api/cluster/quorum', methods=['GET'])
def get_quorum():
    """Get current quorum configuration."""
    return jsonify(cluster.quorum.to_dict())


@app.route('/api/cluster/quorum', methods=['POST'])
def set_quorum():
    """Update quorum configuration."""
    data = request.json or {}
    n = data.get('n', cluster.quorum.n)
    r = data.get('r', cluster.quorum.r)
    w = data.get('w', cluster.quorum.w)
    cluster.quorum = QuorumConfig(n=n, r=r, w=w)
    return jsonify(cluster.quorum.to_dict())


# --- Data Operations ---

@app.route('/api/data/write', methods=['POST'])
def write_object():
    """Write an object to the cluster."""
    data = request.json or {}
    key = data.get('key')
    value = data.get('value')

    if not key or value is None:
        return jsonify({'error': 'key and value required'}), 400

    result = cluster.write(key, str(value))
    return jsonify(result.to_dict())


@app.route('/api/data/read', methods=['GET'])
def read_object():
    """Read an object from the cluster."""
    key = request.args.get('key')
    if not key:
        return jsonify({'error': 'key parameter required'}), 400

    result = cluster.read(key)
    return jsonify(result.to_dict())


@app.route('/api/data/replicas', methods=['GET'])
def get_replicas():
    """Get replica information for an object."""
    key = request.args.get('key')
    if not key:
        return jsonify({'error': 'key parameter required'}), 400

    result = cluster.get_object_replicas(key)
    return jsonify(result)


@app.route('/api/data/distribution', methods=['GET'])
def get_distribution():
    """Get object distribution across nodes."""
    return jsonify(cluster.get_object_distribution())


@app.route('/api/data/bulk-write', methods=['POST'])
def bulk_write():
    """Write multiple objects at once."""
    data = request.json or {}
    count = data.get('count', 50)
    prefix = data.get('prefix', 'obj')

    results = {'success': 0, 'failed': 0, 'hinted': 0}
    for i in range(count):
        key = f"{prefix}-{i}"
        value = f"data-payload-{i}-{time.time()}"
        result = cluster.write(key, value)
        if result.success:
            results['success'] += 1
            if result.hinted_handoffs:
                results['hinted'] += len(result.hinted_handoffs)
        else:
            results['failed'] += 1

    results['total'] = count
    return jsonify(results)


# --- Background Operations ---

@app.route('/api/ops/gossip', methods=['POST'])
def run_gossip():
    """Manually trigger a gossip round."""
    cluster.run_gossip_round()
    return jsonify({'status': 'gossip round completed'})


@app.route('/api/ops/anti-entropy', methods=['POST'])
def run_anti_entropy():
    """Run anti-entropy synchronization."""
    result = cluster.run_anti_entropy()
    return jsonify(result)


@app.route('/api/ops/scrub', methods=['POST'])
def run_scrub():
    """Run integrity scrub on all nodes."""
    result = cluster.run_scrub()
    return jsonify(result)


# --- Chaos Operations ---

@app.route('/api/chaos/corrupt', methods=['POST'])
def inject_corruption():
    """Inject random data corruption."""
    result = cluster.corrupt_random_object()
    return jsonify(result)


@app.route('/api/chaos/kill-random', methods=['POST'])
def kill_random_node():
    """Kill a random alive node."""
    alive = [n for n in cluster._node_states
             if cluster._node_states[n] == 'alive']
    if not alive:
        return jsonify({'error': 'No alive nodes'})

    import random
    node_id = random.choice(alive)
    result = cluster.kill_node(node_id)
    return jsonify(result)


# --- Ring Visualization ---

@app.route('/api/ring/state', methods=['GET'])
def get_ring_state():
    """Get hash ring state for visualization."""
    return jsonify({
        'ring': cluster.hash_ring.get_ring_state(),
        'nodes': cluster.hash_ring.nodes,
        'vnodes_per_node': cluster.hash_ring.vnodes_per_node,
        'total_vnodes': cluster.hash_ring.total_vnodes,
    })


@app.route('/api/ring/lookup', methods=['GET'])
def ring_lookup():
    """Look up which node a key maps to."""
    key = request.args.get('key', '')
    if not key:
        return jsonify({'error': 'key parameter required'}), 400

    coordinator = cluster.hash_ring.get_node(key)
    replicas = cluster.hash_ring.get_replica_nodes(key, cluster.quorum.n)
    position = cluster.hash_ring.get_key_position(key)

    return jsonify({
        'key': key,
        'position': position,
        'coordinator': coordinator,
        'replicas': replicas,
    })


if __name__ == '__main__':
    print("\n" + "=" * 60)
    print("  VAULT - Distributed Object Storage System")
    print("  Dashboard: http://localhost:5555")
    print("=" * 60 + "\n")
    app.run(host='0.0.0.0', port=5555, debug=False)
