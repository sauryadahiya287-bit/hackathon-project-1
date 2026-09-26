# -*- coding: utf-8 -*-
"""
End-to-end integration test for Vault distributed storage cluster.
Tests: bulk write, read, key lookup, chaos (kill/corrupt/revive), 
hinted handoff, anti-entropy, scrub, and read repair.
"""

import urllib.request
import json
import sys
import io

# Force UTF-8 output
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

API = 'http://localhost:5555'

def api_post(endpoint, data=None):
    body = json.dumps(data or {}).encode()
    req = urllib.request.Request(f'{API}{endpoint}', data=body,
                                headers={'Content-Type': 'application/json'})
    r = urllib.request.urlopen(req)
    return json.loads(r.read())

def api_get(endpoint):
    r = urllib.request.urlopen(f'{API}{endpoint}')
    return json.loads(r.read())

def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

passed = 0
failed = 0

def check(label, condition):
    global passed, failed
    if condition:
        passed += 1
        print(f"  [PASS] {label}")
    else:
        failed += 1
        print(f"  [FAIL] {label}")

# --------------------------------------------------
section("1. INITIALIZE CLUSTER (5 nodes)")
result = api_post('/api/cluster/init', {'num_nodes': 5})
check("Cluster created with 5 nodes", result.get('nodes_created') == 5)

# --------------------------------------------------
section("2. BULK WRITE - 100 objects")
result = api_post('/api/data/bulk-write', {'count': 100, 'prefix': 'test'})
check(f"All 100 writes succeeded (got {result.get('success', 0)})", result.get('success') == 100)
check(f"No failed writes", result.get('failed') == 0)
print(f"  Hinted handoffs: {result.get('hinted', 0)}")

# --------------------------------------------------
section("3. READ - Verify data integrity")
read_ok = 0
for i in [0, 25, 50, 75, 99]:
    result = api_get(f'/api/data/read?key=test-{i}')
    if result.get('success'):
        read_ok += 1
        repair = " [READ REPAIR]" if result.get('read_repair_triggered') else ""
        print(f"  test-{i}: responses={result['responses']}, "
              f"nodes={result.get('responding_nodes', [])}{repair}")
check(f"All 5 sample reads succeeded ({read_ok}/5)", read_ok == 5)

# Verify no false conflicts
result = api_get('/api/data/read?key=test-42')
check("No false vector clock conflicts", len(result.get('conflicts', [])) == 0)

# --------------------------------------------------
section("4. KEY LOOKUP - Hash ring routing")
for key in ['user:12345', 'order:99999', 'session:abc']:
    result = api_get(f'/api/ring/lookup?key={key}')
    check(f"Key '{key}' routed to coordinator {result.get('coordinator')}",
          result.get('coordinator') is not None)
    n_replicas = len(result.get('replicas', []))
    check(f"  Has 3 replicas ({n_replicas})", n_replicas == 3)

# --------------------------------------------------
section("5. OBJECT DISTRIBUTION")
dist = api_get('/api/data/distribution')
total_objects = sum(v.get('count', 0) for v in dist.values())
for node_id, info in dist.items():
    bar = "#" * (info['count'] // 2)
    print(f"  {node_id}: {info['count']:3d} objects {bar}")
check(f"Total distributed objects > 0 (total={total_objects})", total_objects > 0)

# --------------------------------------------------
section("6. CHAOS - Kill a node")
state_before = api_get('/api/cluster/state')
alive_before = state_before.get('alive_nodes', 0)

result = api_post('/api/chaos/kill-random')
killed_node = result.get('node_id', 'unknown')
print(f"  Killed: {killed_node}")
check("Node killed successfully", result.get('state') == 'dead')

# Test reads still work with one node down
print("  Reading with node down...")
read_ok = 0
for i in range(20):
    result = api_get(f'/api/data/read?key=test-{i}')
    if result.get('success'):
        read_ok += 1
check(f"Reads still work after node failure ({read_ok}/20 succeeded)", read_ok >= 15)

# --------------------------------------------------
section("7. WRITE DURING FAILURE - Hinted Handoff")
result = api_post('/api/data/write', {'key': 'during-failure', 'value': 'written while node is down'})
check("Write succeeded during node failure", result.get('success') == True)
print(f"  Acks: {result.get('acks')}/{result.get('required_acks')}")
print(f"  Hinted handoffs: {result.get('hinted_handoffs', [])}")

# --------------------------------------------------
section("8. REVIVE NODE - Hinted Handoff Delivery")
result = api_post('/api/cluster/revive-node', {'node_id': killed_node})
check(f"Node {killed_node} revived", result.get('state') == 'alive')
hints = result.get('hints_delivered', 0)
print(f"  Hints delivered: {hints}")

# --------------------------------------------------
section("9. CORRUPTION INJECTION & SCRUB")
result = api_post('/api/chaos/corrupt')
corrupted_key = result.get('key', 'none')
corrupted_node = result.get('node_id', 'none')
check(f"Corruption injected: {corrupted_key} on {corrupted_node}", result.get('corrupted') == True)

result = api_post('/api/ops/scrub')
check(f"Scrub detected corruption ({result.get('corruptions_found', 0)} found)",
      result.get('corruptions_found', 0) >= 1)
print(f"  Nodes scrubbed: {result.get('nodes_scrubbed')}")

# Verify the corrupted object was auto-repaired
result2 = api_get(f'/api/data/read?key={corrupted_key}')
check(f"Corrupted object readable after auto-repair", result2.get('success') == True)

# --------------------------------------------------
section("10. ANTI-ENTROPY SYNC")
result = api_post('/api/ops/anti-entropy')
check(f"Anti-entropy checked {result.get('pairs_checked', 0)} pairs",
      result.get('pairs_checked', 0) > 0)
print(f"  In sync: {result.get('in_sync', 0)}")
print(f"  Repairs made: {result.get('repairs_made', 0)}")

# --------------------------------------------------
section("11. QUORUM RECONFIGURATION")
result = api_post('/api/cluster/quorum', {'n': 3, 'r': 1, 'w': 1})
check("Quorum set to N=3, R=1, W=1 (eventual consistency)",
      result.get('consistency_level') == 'eventual')

result = api_post('/api/cluster/quorum', {'n': 3, 'r': 2, 'w': 2})
check("Quorum set to N=3, R=2, W=2 (strong consistency)",
      result.get('consistency_level') == 'strong')

# --------------------------------------------------
section("12. CLUSTER STATE SUMMARY")
state = api_get('/api/cluster/state')
m = state.get('metrics', {})
print(f"  Total nodes:     {state.get('total_nodes')}")
print(f"  Alive nodes:     {state.get('alive_nodes')}")
print(f"  Dead nodes:      {state.get('dead_nodes')}")
q = state.get('quorum', {})
print(f"  Quorum:          N={q.get('n')}, R={q.get('r')}, W={q.get('w')} ({q.get('consistency_level')})")
print(f"  Total writes:    {m.get('total_writes', 0)} (success: {m.get('successful_writes', 0)})")
print(f"  Total reads:     {m.get('total_reads', 0)} (success: {m.get('successful_reads', 0)})")
print(f"  Read repairs:    {m.get('total_read_repairs', 0)}")
print(f"  Hinted handoffs: {m.get('total_hinted_handoffs', 0)}")
print(f"  Anti-entropy:    {m.get('total_anti_entropy_syncs', 0)} syncs")
print(f"  Scrubs:          {m.get('total_scrubs', 0)}")
print(f"  Corruptions:     {m.get('total_corruptions_detected', 0)}")
print(f"  Events logged:   {len(state.get('events', []))}")

check("Cluster fully operational", state.get('alive_nodes', 0) == state.get('total_nodes', 0))

# --------------------------------------------------
print(f"\n{'='*60}")
if failed == 0:
    print(f"  ALL {passed} TESTS PASSED - Vault is operational!")
else:
    print(f"  {passed} passed, {failed} FAILED")
print(f"{'='*60}")

sys.exit(0 if failed == 0 else 1)
