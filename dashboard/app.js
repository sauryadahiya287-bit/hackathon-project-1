/* ========================================================
   VAULT Dashboard — Application Logic
   Real-time visualization of the distributed storage cluster
   ======================================================== */

const API = '';
let clusterState = null;
let ringLabelsVisible = true;
let ringAnimationEnabled = true;
let ringAnimationFrame = null;
let ringAngle = 0;
let currentEventFilter = 'all';
let pollingInterval = null;

// Node color palette
const NODE_COLORS = [
    '#818cf8', '#6ee7b7', '#f472b6', '#fbbf24', '#60a5fa',
    '#fb923c', '#a78bfa', '#5eead4', '#f87171', '#34d399',
    '#e879f9', '#38bdf8', '#a3e635', '#facc15', '#fb7185',
    '#22d3ee', '#c084fc', '#4ade80', '#f97316', '#ec4899',
];

function getNodeColor(index) {
    return NODE_COLORS[index % NODE_COLORS.length];
}

// ========================================================
// TAB NAVIGATION
// ========================================================
document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        const tabId = btn.dataset.tab;

        document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));

        btn.classList.add('active');
        document.getElementById(`tab-${tabId}`).classList.add('active');

        if (tabId === 'ring') drawHashRing();
        if (tabId === 'events') renderEvents();
        if (tabId === 'operations') updateDistribution();
    });
});

// ========================================================
// API HELPERS
// ========================================================
async function apiPost(endpoint, data = {}) {
    try {
        const res = await fetch(`${API}${endpoint}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data),
        });
        return await res.json();
    } catch (err) {
        showToast(`API Error: ${err.message}`, 'error');
        return { error: err.message };
    }
}

async function apiGet(endpoint) {
    try {
        const res = await fetch(`${API}${endpoint}`);
        return await res.json();
    } catch (err) {
        showToast(`API Error: ${err.message}`, 'error');
        return { error: err.message };
    }
}

// ========================================================
// TOAST NOTIFICATIONS
// ========================================================
function showToast(message, type = 'success', duration = 3500) {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;

    const icon = type === 'success' ? '✓' : type === 'error' ? '✗' : '⚠';
    toast.innerHTML = `<span style="font-size:1.1rem">${icon}</span><span>${message}</span>`;

    container.appendChild(toast);

    setTimeout(() => {
        toast.style.animation = 'toastOut 0.3s ease-out forwards';
        setTimeout(() => toast.remove(), 300);
    }, duration);
}

// ========================================================
// CLUSTER INITIALIZATION
// ========================================================
async function initCluster() {
    const numNodes = parseInt(document.getElementById('init-nodes').value) || 5;
    const btn = document.getElementById('btn-init-cluster');
    btn.disabled = true;
    btn.textContent = 'Initializing...';

    const result = await apiPost('/api/cluster/init', { num_nodes: numNodes });

    btn.disabled = false;
    btn.innerHTML = `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="5 3 19 12 5 21 5 3"/></svg> Initialize Cluster`;

    if (result.nodes_created) {
        showToast(`Cluster initialized with ${result.nodes_created} nodes`, 'success');
        document.getElementById('cluster-status-badge').textContent = 'Active';
        document.getElementById('cluster-status-badge').className = 'badge badge-green';
        startPolling();
        refreshAll();
    } else {
        showToast('Failed to initialize cluster', 'error');
    }
}

async function addNode() {
    const result = await apiPost('/api/cluster/add-node');
    if (result.node_id) {
        showToast(`Node ${result.node_id} added`, 'success');
        refreshAll();
    }
}

// ========================================================
// QUORUM CONFIGURATION
// ========================================================
function updateQuorum() {
    const n = parseInt(document.getElementById('quorum-n').value);
    const r = parseInt(document.getElementById('quorum-r').value);
    const w = parseInt(document.getElementById('quorum-w').value);

    document.getElementById('quorum-n-val').textContent = n;
    document.getElementById('quorum-r-val').textContent = r;
    document.getElementById('quorum-w-val').textContent = w;
    document.getElementById('rw-sum').textContent = r + w;
    document.getElementById('n-val-formula').textContent = n;

    const isStrict = r + w > n;
    const formulaResult = document.getElementById('formula-result');
    const badge = document.getElementById('consistency-badge');

    if (isStrict) {
        formulaResult.className = 'text-green';
        formulaResult.textContent = 'Strong Consistency ✓';
        badge.className = 'badge badge-green';
        badge.textContent = 'Strong';
    } else {
        formulaResult.className = 'text-red';
        formulaResult.textContent = 'Eventual Consistency ⚡';
        badge.className = 'badge badge-orange';
        badge.textContent = 'Eventual';
    }

    // Send to server (debounced)
    clearTimeout(updateQuorum._timeout);
    updateQuorum._timeout = setTimeout(() => {
        apiPost('/api/cluster/quorum', { n, r, w });
    }, 500);
}

// ========================================================
// NODE GRID
// ========================================================
function renderNodeGrid(nodes) {
    const grid = document.getElementById('node-grid');
    if (!nodes || nodes.length === 0) {
        grid.innerHTML = `<div class="empty-state"><p>Initialize the cluster to see nodes</p></div>`;
        return;
    }

    document.getElementById('node-count-badge').textContent = `${nodes.length} nodes`;

    grid.innerHTML = nodes.map((node, i) => {
        const color = getNodeColor(i);
        const isDead = node.state === 'dead';
        const storage = node.storage || {};

        return `
            <div class="node-card ${isDead ? 'dead' : ''}" style="--node-color: ${color}" id="node-${node.node_id}">
                <div class="node-card-header">
                    <span class="node-name" style="color: ${color}">${node.node_id}</span>
                    <span class="node-state">
                        <span class="state-dot ${isDead ? 'dead' : ''}"></span>
                        ${node.state}
                    </span>
                </div>
                <div class="node-stats">
                    <div class="node-stat">
                        <span class="node-stat-label">Objects</span>
                        <span class="node-stat-value">${storage.total_objects || 0}</span>
                    </div>
                    <div class="node-stat">
                        <span class="node-stat-label">Writes</span>
                        <span class="node-stat-value">${storage.total_writes || 0}</span>
                    </div>
                    <div class="node-stat">
                        <span class="node-stat-label">Reads</span>
                        <span class="node-stat-value">${storage.total_reads || 0}</span>
                    </div>
                    <div class="node-stat">
                        <span class="node-stat-label">Stored</span>
                        <span class="node-stat-value">${formatBytes(storage.bytes_stored || 0)}</span>
                    </div>
                    <div class="node-stat">
                        <span class="node-stat-label">Integrity</span>
                        <span class="node-stat-value" style="color: ${(storage.integrity_failures || 0) > 0 ? '#f87171' : '#6ee7b7'}">
                            ${(storage.integrity_failures || 0) > 0 ? '⚠ ' + storage.integrity_failures : '✓ OK'}
                        </span>
                    </div>
                    <div class="node-stat">
                        <span class="node-stat-label">Hints</span>
                        <span class="node-stat-value">${storage.hints_stored || 0}</span>
                    </div>
                </div>
                <div class="node-actions">
                    ${isDead
                        ? `<button class="btn btn-sm btn-success" onclick="reviveNode('${node.node_id}')">Revive</button>`
                        : `<button class="btn btn-sm btn-danger" onclick="killNode('${node.node_id}')">Kill</button>`
                    }
                </div>
            </div>
        `;
    }).join('');
}

// ========================================================
// DATA OPERATIONS
// ========================================================
async function writeObject() {
    const key = document.getElementById('write-key').value.trim();
    const value = document.getElementById('write-value').value.trim();
    const resultDiv = document.getElementById('write-result');

    if (!key || !value) {
        showToast('Key and value are required', 'error');
        return;
    }

    const result = await apiPost('/api/data/write', { key, value });
    resultDiv.className = `ops-result ${result.success ? 'success' : 'error'}`;
    resultDiv.innerHTML = formatResult(result);
    showToast(result.success ? `Written: ${key}` : `Write failed: ${result.error}`, result.success ? 'success' : 'error');
    refreshAll();
}

async function readObject() {
    const key = document.getElementById('read-key').value.trim();
    const resultDiv = document.getElementById('read-result');

    if (!key) {
        showToast('Key is required', 'error');
        return;
    }

    const result = await apiGet(`/api/data/read?key=${encodeURIComponent(key)}`);
    resultDiv.className = `ops-result ${result.success ? 'success' : 'error'}`;
    resultDiv.innerHTML = formatResult(result);
    showToast(result.success ? `Read: ${key}` : `Read failed: ${result.error}`, result.success ? 'success' : 'error');
}

async function bulkWrite() {
    const count = parseInt(document.getElementById('bulk-count').value) || 50;
    const prefix = document.getElementById('bulk-prefix').value || 'obj';
    const resultDiv = document.getElementById('bulk-result');

    resultDiv.innerHTML = '<span style="color: var(--accent-amber)">Writing...</span>';

    const result = await apiPost('/api/data/bulk-write', { count, prefix });
    resultDiv.className = 'ops-result success';
    resultDiv.innerHTML = formatResult(result);
    showToast(`Bulk write: ${result.success || 0}/${result.total || 0} succeeded`, 'success');
    refreshAll();
}

// ========================================================
// BACKGROUND OPERATIONS
// ========================================================
async function runAntiEntropy() {
    const result = await apiPost('/api/ops/anti-entropy');
    const resultDiv = document.getElementById('bg-result') || document.getElementById('chaos-result');
    if (resultDiv) {
        resultDiv.className = 'ops-result success';
        resultDiv.innerHTML = formatResult(result);
    }
    showToast(`Anti-entropy: ${result.repairs_made || 0} repairs made`, 'success');
    refreshAll();
}

async function runScrub() {
    const result = await apiPost('/api/ops/scrub');
    const resultDiv = document.getElementById('bg-result') || document.getElementById('chaos-result');
    if (resultDiv) {
        resultDiv.className = `ops-result ${result.corruptions_found > 0 ? 'error' : 'success'}`;
        resultDiv.innerHTML = formatResult(result);
    }
    showToast(`Scrub: ${result.corruptions_found || 0} corruptions found`, result.corruptions_found > 0 ? 'warning' : 'success');
    refreshAll();
}

// ========================================================
// CHAOS OPERATIONS
// ========================================================
async function killNode(nodeId) {
    const result = await apiPost('/api/cluster/kill-node', { node_id: nodeId });
    showToast(`💀 Node ${nodeId} killed!`, 'error');
    refreshAll();
}

async function reviveNode(nodeId) {
    const result = await apiPost('/api/cluster/revive-node', { node_id: nodeId });
    showToast(`✨ Node ${nodeId} revived! ${result.hints_delivered || 0} hints delivered`, 'success');
    refreshAll();
}

async function chaosKillRandom() {
    const result = await apiPost('/api/chaos/kill-random');
    if (result.node_id) {
        showToast(`⚡ Random kill: ${result.node_id}`, 'error');
        document.getElementById('chaos-result').innerHTML = formatResult(result);
    }
    refreshAll();
}

async function chaosCorrupt() {
    const result = await apiPost('/api/chaos/corrupt');
    if (result.corrupted) {
        showToast(`☠ Data corrupted: ${result.key} on ${result.node_id}`, 'warning');
        document.getElementById('chaos-result').innerHTML = formatResult(result);
    } else {
        showToast(result.error || 'Nothing to corrupt', 'warning');
    }
    refreshAll();
}

async function chaosKillMultiple() {
    for (let i = 0; i < 2; i++) {
        await apiPost('/api/chaos/kill-random');
        await new Promise(r => setTimeout(r, 200));
    }
    showToast('⚡⚡ Multiple nodes killed!', 'error');
    refreshAll();
}

async function chaosReviveAll() {
    if (!clusterState) return;
    const deadNodes = clusterState.nodes.filter(n => n.state === 'dead');
    for (const node of deadNodes) {
        await apiPost('/api/cluster/revive-node', { node_id: node.node_id });
    }
    showToast(`✨ Revived ${deadNodes.length} nodes`, 'success');
    refreshAll();
}

async function chaosKillTarget() {
    const nodeId = document.getElementById('chaos-target-node').value;
    if (!nodeId) return showToast('Select a node', 'warning');
    await killNode(nodeId);
}

async function chaosReviveTarget() {
    const nodeId = document.getElementById('chaos-target-node').value;
    if (!nodeId) return showToast('Select a node', 'warning');
    await reviveNode(nodeId);
}

// ========================================================
// KEY LOOKUP
// ========================================================
async function lookupKey() {
    const key = document.getElementById('lookup-key').value.trim();
    if (!key) return;

    const result = await apiGet(`/api/ring/lookup?key=${encodeURIComponent(key)}`);
    const div = document.getElementById('lookup-result');

    if (result.coordinator) {
        div.innerHTML = `
<span style="color:var(--text-muted)">Key:</span> <code>${result.key}</code>
<span style="color:var(--text-muted)">Position:</span> ${(result.position * 100).toFixed(4)}%
<span style="color:var(--text-muted)">Coordinator:</span> <span style="color:var(--accent-indigo)">${result.coordinator}</span>
<span style="color:var(--text-muted)">Replicas:</span> ${result.replicas.map(r => `<span style="color:var(--accent-emerald)">${r}</span>`).join(', ')}
        `.trim();
        drawHashRing(result.position, result.replicas);
    }
}

// ========================================================
// HASH RING CANVAS VISUALIZATION
// ========================================================
function drawHashRing(highlightPos = null, highlightNodes = []) {
    const canvas = document.getElementById('ring-canvas');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');

    const dpr = window.devicePixelRatio || 1;
    const displaySize = 600;
    canvas.width = displaySize * dpr;
    canvas.height = displaySize * dpr;
    canvas.style.width = displaySize + 'px';
    canvas.style.height = displaySize + 'px';
    ctx.scale(dpr, dpr);

    const cx = displaySize / 2;
    const cy = displaySize / 2;
    const radius = 220;

    // Clear
    ctx.clearRect(0, 0, displaySize, displaySize);

    // Background ring glow
    ctx.beginPath();
    ctx.arc(cx, cy, radius, 0, Math.PI * 2);
    ctx.strokeStyle = 'rgba(129, 140, 248, 0.06)';
    ctx.lineWidth = 30;
    ctx.stroke();

    // Main ring
    ctx.beginPath();
    ctx.arc(cx, cy, radius, 0, Math.PI * 2);
    ctx.strokeStyle = 'rgba(129, 140, 248, 0.15)';
    ctx.lineWidth = 2;
    ctx.stroke();

    // Inner decorative ring
    ctx.beginPath();
    ctx.arc(cx, cy, radius - 25, 0, Math.PI * 2);
    ctx.strokeStyle = 'rgba(255, 255, 255, 0.03)';
    ctx.lineWidth = 1;
    ctx.setLineDash([4, 8]);
    ctx.stroke();
    ctx.setLineDash([]);

    if (!clusterState || !clusterState.ring || clusterState.ring.length === 0) {
        // Empty state
        ctx.fillStyle = 'rgba(255, 255, 255, 0.15)';
        ctx.font = '500 16px Inter';
        ctx.textAlign = 'center';
        ctx.fillText('Initialize cluster to see the hash ring', cx, cy);
        return;
    }

    // Build node index map
    const nodeList = [...new Set(clusterState.ring.map(v => v.physical_node))];
    const nodeColorMap = {};
    nodeList.forEach((nid, i) => { nodeColorMap[nid] = getNodeColor(i); });

    // Draw vnodes as dots on the ring
    const ringData = clusterState.ring;
    ringData.forEach(vnode => {
        const angle = vnode.position_normalized * Math.PI * 2 - Math.PI / 2 + (ringAnimationEnabled ? ringAngle : 0);
        const x = cx + radius * Math.cos(angle);
        const y = cy + radius * Math.sin(angle);

        const color = nodeColorMap[vnode.physical_node] || '#818cf8';
        const isHighlighted = highlightNodes.includes(vnode.physical_node);

        ctx.beginPath();
        ctx.arc(x, y, isHighlighted ? 5 : 3, 0, Math.PI * 2);
        ctx.fillStyle = color;
        ctx.globalAlpha = isHighlighted ? 1 : 0.5;
        ctx.fill();
        ctx.globalAlpha = 1;

        if (isHighlighted) {
            ctx.beginPath();
            ctx.arc(x, y, 10, 0, Math.PI * 2);
            ctx.strokeStyle = color;
            ctx.lineWidth = 1.5;
            ctx.globalAlpha = 0.4;
            ctx.stroke();
            ctx.globalAlpha = 1;
        }
    });

    // Draw highlighted key position
    if (highlightPos !== null) {
        const angle = highlightPos * Math.PI * 2 - Math.PI / 2 + (ringAnimationEnabled ? ringAngle : 0);
        const x = cx + radius * Math.cos(angle);
        const y = cy + radius * Math.sin(angle);

        // Key marker
        ctx.beginPath();
        ctx.arc(x, y, 8, 0, Math.PI * 2);
        ctx.fillStyle = '#ffffff';
        ctx.fill();

        ctx.beginPath();
        ctx.arc(x, y, 12, 0, Math.PI * 2);
        ctx.strokeStyle = '#ffffff';
        ctx.lineWidth = 2;
        ctx.globalAlpha = 0.5;
        ctx.stroke();
        ctx.globalAlpha = 1;

        // Line from center to key
        ctx.beginPath();
        ctx.moveTo(cx, cy);
        ctx.lineTo(x, y);
        ctx.strokeStyle = 'rgba(255, 255, 255, 0.15)';
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 4]);
        ctx.stroke();
        ctx.setLineDash([]);
    }

    // Draw node labels around the ring
    if (ringLabelsVisible) {
        const labelRadius = radius + 45;
        nodeList.forEach((nid, i) => {
            // Find the average position of this node's vnodes
            const nodeVnodes = ringData.filter(v => v.physical_node === nid);
            if (nodeVnodes.length === 0) return;

            // Use the first vnode position for label placement
            const avgPos = nodeVnodes[0].position_normalized;
            const angle = avgPos * Math.PI * 2 - Math.PI / 2 + (ringAnimationEnabled ? ringAngle : 0);
            const x = cx + labelRadius * Math.cos(angle);
            const y = cy + labelRadius * Math.sin(angle);

            const color = nodeColorMap[nid];
            const isDead = clusterState.nodes?.find(n => n.node_id === nid)?.state === 'dead';

            ctx.fillStyle = isDead ? 'rgba(248, 113, 113, 0.7)' : color;
            ctx.font = `600 11px 'Inter'`;
            ctx.textAlign = 'center';
            ctx.textBaseline = 'middle';
            ctx.globalAlpha = isDead ? 0.5 : 0.85;
            ctx.fillText(nid.replace('vault-node-', 'N'), x, y);
            ctx.globalAlpha = 1;
        });
    }

    // Center info
    ctx.fillStyle = 'rgba(255, 255, 255, 0.7)';
    ctx.font = '700 20px Inter';
    ctx.textAlign = 'center';
    ctx.fillText(`${nodeList.length}`, cx, cy - 12);
    ctx.fillStyle = 'rgba(255, 255, 255, 0.35)';
    ctx.font = '500 11px Inter';
    ctx.fillText('NODES', cx, cy + 6);
    ctx.fillText(`${ringData.length} vnodes`, cx, cy + 22);

    // Update stats
    document.getElementById('ring-total-vnodes').textContent = ringData.length;
    document.getElementById('ring-phys-nodes').textContent = nodeList.length;
}

function toggleRingLabels() {
    ringLabelsVisible = !ringLabelsVisible;
    drawHashRing();
}

function toggleRingAnimation() {
    ringAnimationEnabled = !ringAnimationEnabled;
    if (ringAnimationEnabled) {
        animateRing();
    }
}

function animateRing() {
    if (!ringAnimationEnabled) return;
    ringAngle += 0.001;
    drawHashRing();
    ringAnimationFrame = requestAnimationFrame(animateRing);
}

// ========================================================
// DISTRIBUTION CHART
// ========================================================
async function updateDistribution() {
    const data = await apiGet('/api/data/distribution');
    const chart = document.getElementById('distribution-chart');

    const entries = Object.entries(data);
    if (entries.length === 0) {
        chart.innerHTML = '<div class="empty-state small"><p>Write objects to see distribution</p></div>';
        return;
    }

    const maxCount = Math.max(...entries.map(([, v]) => v.count), 1);

    chart.innerHTML = entries.map(([nodeId, info], i) => {
        const color = getNodeColor(i);
        const heightPct = (info.count / maxCount) * 100;
        const isDead = info.state === 'dead';

        return `
            <div class="dist-bar-wrapper">
                <div class="dist-count">${info.count}</div>
                <div class="dist-bar" style="height: ${Math.max(heightPct, 3)}%; background: ${isDead ? '#f87171' : color}; opacity: ${isDead ? 0.4 : 1}"></div>
                <div class="dist-label">${nodeId.replace('vault-node-', 'N')}</div>
            </div>
        `;
    }).join('');
}

// ========================================================
// EVENT LOG
// ========================================================
function renderEvents() {
    if (!clusterState || !clusterState.events) return;

    const list = document.getElementById('event-list');
    let events = [...clusterState.events].reverse();

    if (currentEventFilter !== 'all') {
        events = events.filter(e => {
            const type = e.type || '';
            switch (currentEventFilter) {
                case 'write': return type.includes('write');
                case 'read': return type.includes('read');
                case 'repair': return type.includes('repair') || type.includes('entropy') || type.includes('hint');
                case 'chaos': return type.includes('kill') || type.includes('corrupt') || type.includes('revive');
                default: return true;
            }
        });
    }

    if (events.length === 0) {
        list.innerHTML = '<div class="empty-state"><p>No matching events</p></div>';
        return;
    }

    list.innerHTML = events.slice(0, 100).map(event => {
        const time = new Date(event.timestamp * 1000).toLocaleTimeString();
        const typeClass = getEventTypeClass(event.type);
        const details = formatEventDetails(event);

        return `
            <div class="event-item">
                <span class="event-time">${time}</span>
                <span class="event-type ${typeClass}">${event.type}</span>
                <span class="event-details">${details}</span>
            </div>
        `;
    }).join('');
}

function getEventTypeClass(type) {
    if (type.includes('write')) return 'write';
    if (type.includes('read')) return 'read';
    if (type.includes('repair') || type.includes('entropy') || type.includes('hint') || type.includes('scrub')) return 'repair';
    if (type.includes('kill') || type.includes('corrupt') || type.includes('dead')) return 'chaos';
    if (type.includes('node') || type.includes('revive')) return 'node';
    return '';
}

function formatEventDetails(event) {
    const parts = [];
    if (event.key) parts.push(`key: <code>${event.key}</code>`);
    if (event.node_id) parts.push(`node: <code>${event.node_id}</code>`);
    if (event.coordinator) parts.push(`coord: <code>${event.coordinator}</code>`);
    if (event.acks !== undefined) parts.push(`acks: ${event.acks}`);
    if (event.responses !== undefined) parts.push(`responses: ${event.responses}`);
    if (event.repairs_made !== undefined) parts.push(`repairs: ${event.repairs_made}`);
    if (event.from) parts.push(`from: <code>${event.from}</code>`);
    if (event.to) parts.push(`to: <code>${event.to}</code>`);
    if (event.target) parts.push(`target: <code>${event.target}</code>`);
    if (event.message) parts.push(event.message);
    if (event.size) parts.push(`${formatBytes(event.size)}`);
    if (event.hints_delivered) parts.push(`hints: ${event.hints_delivered}`);
    if (event.corrupted_node) parts.push(`corrupted: <code>${event.corrupted_node}</code>`);
    if (event.repaired_from) parts.push(`repaired from: <code>${event.repaired_from}</code>`);
    return parts.join(' · ') || JSON.stringify(event).slice(0, 120);
}

function filterEvents(filter) {
    currentEventFilter = filter;
    document.querySelectorAll('.filter-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.filter === filter);
    });
    renderEvents();
}

// ========================================================
// HEALTH INDICATORS
// ========================================================
function updateHealth() {
    if (!clusterState) return;

    const total = clusterState.total_nodes || 1;
    const alive = clusterState.alive_nodes || 0;
    const availability = (alive / total) * 100;

    document.getElementById('health-availability').style.width = `${availability}%`;
    document.getElementById('health-availability-val').textContent = `${Math.round(availability)}%`;

    // Update bar color based on level
    const bar = document.getElementById('health-availability');
    if (availability >= 80) bar.style.background = 'linear-gradient(90deg, #6ee7b7, #34d399)';
    else if (availability >= 50) bar.style.background = 'linear-gradient(90deg, #fbbf24, #f59e0b)';
    else bar.style.background = 'linear-gradient(90deg, #f87171, #ef4444)';

    // Integrity
    const corruptions = clusterState.metrics?.total_corruptions_detected || 0;
    const integrityPct = corruptions > 0 ? Math.max(50, 100 - corruptions * 5) : 100;
    document.getElementById('health-integrity').style.width = `${integrityPct}%`;
    document.getElementById('health-integrity-val').textContent = `${Math.round(integrityPct)}%`;

    // Replication
    const writes = clusterState.metrics?.successful_writes || 0;
    const repPct = writes > 0 ? Math.min(100, 60 + writes) : 0;
    document.getElementById('health-replication').style.width = `${repPct}%`;
    document.getElementById('health-replication-val').textContent = `${Math.round(repPct)}%`;

    // Update chaos node selector
    const select = document.getElementById('chaos-target-node');
    const currentVal = select.value;
    select.innerHTML = '<option value="">Select a node...</option>' +
        (clusterState.nodes || []).map(n =>
            `<option value="${n.node_id}" ${n.state === 'dead' ? 'style="color:#f87171"' : ''}>${n.node_id} (${n.state})</option>`
        ).join('');
    select.value = currentVal;
}

// ========================================================
// HERO STATS
// ========================================================
function updateHeroStats() {
    if (!clusterState) return;

    const metrics = clusterState.metrics || {};
    const totalObjects = (clusterState.nodes || []).reduce(
        (sum, n) => sum + (n.storage?.total_objects || 0), 0
    );

    animateNumber('stat-nodes', clusterState.alive_nodes || 0);
    animateNumber('stat-objects', totalObjects);
    animateNumber('stat-writes', metrics.successful_writes || 0);
    animateNumber('stat-reads', metrics.successful_reads || 0);
    animateNumber('stat-repairs', (metrics.total_read_repairs || 0) + (metrics.total_hinted_handoffs || 0));
}

function animateNumber(elementId, target) {
    const el = document.getElementById(elementId);
    const current = parseInt(el.textContent) || 0;
    if (current === target) return;

    const diff = target - current;
    const step = Math.ceil(Math.abs(diff) / 10);
    let val = current;

    function tick() {
        if (diff > 0) val = Math.min(val + step, target);
        else val = Math.max(val - step, target);
        el.textContent = val;
        if (val !== target) requestAnimationFrame(tick);
    }
    tick();
}

// ========================================================
// UTILITIES
// ========================================================
function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function formatResult(obj) {
    return Object.entries(obj).map(([k, v]) => {
        const val = typeof v === 'object' ? JSON.stringify(v) : v;
        return `<span style="color:var(--text-muted)">${k}:</span> <span style="color:var(--accent-violet)">${val}</span>`;
    }).join('\n');
}

// ========================================================
// POLLING & REFRESH
// ========================================================
async function refreshAll() {
    try {
        clusterState = await apiGet('/api/cluster/state');
        if (clusterState) {
            renderNodeGrid(clusterState.nodes);
            updateHeroStats();
            updateHealth();
            renderEvents();
            drawHashRing();
            updateDistribution();
        }
    } catch (err) {
        // Silently fail
    }
}

function startPolling() {
    if (pollingInterval) clearInterval(pollingInterval);
    pollingInterval = setInterval(refreshAll, 2000);
}

// Initial load
refreshAll();
