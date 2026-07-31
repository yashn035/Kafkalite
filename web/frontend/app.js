const API_BASE = '/api';
let jwtToken = localStorage.getItem('kafkalite_jwt') || '';
let chart;

// Authentication Modal
const authModal = new bootstrap.Modal(document.getElementById('authModal'));

document.addEventListener('DOMContentLoaded', () => {
    if (!jwtToken) {
        authModal.show();
    } else {
        initWebSocket();
        fetchTopics();
    }

    document.getElementById('saveTokenBtn').addEventListener('click', () => {
        const t = document.getElementById('jwtToken').value.trim();
        if (t) {
            jwtToken = t;
            localStorage.setItem('kafkalite_jwt', t);
            authModal.hide();
            initWebSocket();
            fetchTopics();
        }
    });

    document.getElementById('logoutBtn').addEventListener('click', () => {
        localStorage.removeItem('kafkalite_jwt');
        location.reload();
    });

    // Navigation
    document.querySelectorAll('.nav-link').forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
            link.classList.add('active');

            const targetId = link.getAttribute('data-target');
            document.querySelectorAll('.page-section').forEach(sec => sec.classList.add('d-none'));
            document.getElementById(targetId).classList.remove('d-none');
            
            document.getElementById('pageTitle').innerText = link.innerText.trim();

            if (targetId === 'topics') fetchTopics();
        });
    });

    // Forms
    document.getElementById('refreshTopicsBtn').addEventListener('click', fetchTopics);
    document.getElementById('produceForm').addEventListener('submit', produceMessage);
    document.getElementById('consumeForm').addEventListener('submit', consumeMessages);
    document.getElementById('schemaForm').addEventListener('submit', registerSchema);
    document.getElementById('refreshInsightsBtn').addEventListener('click', fetchInsights);
    
    // Init Chart
    const ctx = document.getElementById('throughputChart').getContext('2d');
    chart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [
                { label: 'Produce Rate', borderColor: '#0d6efd', data: [], tension: 0.4 },
                { label: 'Consume Rate', borderColor: '#198754', data: [], tension: 0.4 }
            ]
        },
        options: {
            responsive: true,
            animation: false,
            scales: {
                x: { display: false },
                y: { beginAtZero: true, grid: { color: '#333' } }
            },
            plugins: {
                legend: { labels: { color: '#fff' } }
            }
        }
    });
});

function getHeaders() {
    return {
        'Authorization': `Bearer ${jwtToken}`,
        'Content-Type': 'application/json'
    };
}

// WebSocket
function initWebSocket() {
    // Determine WebSocket protocol based on current page protocol (ws or wss for https tunnels)
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsHost = window.location.host;
    const ws = new WebSocket(`${protocol}//${wsHost}/ws/ws/metrics?token=${jwtToken}`);
    const badge = document.getElementById('wsStatus');

    ws.onopen = () => {
        badge.className = 'badge bg-success';
        badge.innerHTML = '<i class="bi bi-circle-fill me-1"></i> Connected';
    };

    ws.onclose = () => {
        badge.className = 'badge bg-danger';
        badge.innerHTML = '<i class="bi bi-circle-fill me-1"></i> Disconnected';
        setTimeout(initWebSocket, 3000);
    };

    ws.onerror = () => ws.close();

    ws.onmessage = (e) => {
        const stats = JSON.parse(e.data);
        document.getElementById('metricProduceRate').innerText = stats.produce_rate || 0;
        document.getElementById('metricConsumeRate').innerText = stats.consume_rate || 0;
        document.getElementById('metricTotalProduced').innerText = stats.messages_produced_total || 0;
        document.getElementById('metricTotalConsumed').innerText = stats.messages_consumed_total || 0;

        const now = new Date().toLocaleTimeString();
        if (chart.data.labels.length > 20) {
            chart.data.labels.shift();
            chart.data.datasets[0].data.shift();
            chart.data.datasets[1].data.shift();
        }
        chart.data.labels.push(now);
        chart.data.datasets[0].data.push(stats.produce_rate || 0);
        chart.data.datasets[1].data.push(stats.consume_rate || 0);
        chart.update();
    };
}

// API Calls
async function fetchTopics() {
    try {
        const res = await fetch(`${API_BASE}/topics`, { headers: getHeaders() });
        if (res.ok) {
            const topics = await res.json();
            const list = document.getElementById('topicList');
            list.innerHTML = '';
            
            if (!topics || topics.length === 0) {
                list.innerHTML = '<li class="list-group-item bg-secondary text-light border-dark">No topics found.</li>';
                return;
            }

            topics.forEach(t => {
                const li = document.createElement('li');
                li.className = 'list-group-item bg-secondary text-light border-dark d-flex justify-content-between align-items-center';
                li.innerHTML = `
                    <div><i class="bi bi-hdd me-2 text-primary"></i> <strong>${t}</strong></div>
                    <span class="badge bg-dark">Available</span>
                `;
                list.appendChild(li);
            });
        } else if (res.status === 401) {
            authModal.show();
        }
    } catch (e) {
        console.error("Failed to fetch topics", e);
    }
}

async function produceMessage(e) {
    e.preventDefault();
    const topic = document.getElementById('pTopic').value;
    const key = document.getElementById('pKey').value;
    const value = document.getElementById('pValue').value;
    const message_id = document.getElementById('pMessageID').value;
    const producer_id = parseInt(document.getElementById('pProducerID').value) || 0;
    const resultDiv = document.getElementById('produceResult');

    try {
        const res = await fetch(`${API_BASE}/produce`, {
            method: 'POST',
            headers: getHeaders(),
            body: JSON.stringify({ topic, key, value, message_id, producer_id })
        });
        
        if (res.ok) {
            const data = await res.json();
            resultDiv.innerHTML = `<div class="alert alert-success">Produced successfully at offset ${data.offset}</div>`;
            document.getElementById('pValue').value = '';
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger">Error producing message</div>`;
        }
    } catch (err) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Network error</div>`;
    }
}

async function consumeMessages(e) {
    e.preventDefault();
    const topic = document.getElementById('cTopic').value;
    const start_time = document.getElementById('cStartTime').value;
    const end_time = document.getElementById('cEndTime').value;
    const resultPre = document.getElementById('consumeResult');

    try {
        resultPre.innerText = "Fetching...";
        let url = `${API_BASE}/consume?topic=${topic}&max=10`;
        if (start_time) url += `&start_time=${start_time}`;
        if (end_time) url += `&end_time=${end_time}`;
        
        const res = await fetch(url, { 
            headers: getHeaders(),
            signal: AbortSignal.timeout(10000)
        });
        if (res.ok) {
            const data = await res.json();
            const records = data.messages || data.records;
            if (records && records.length > 0) {
                const formatted = records.map(r => `[Offset: ${r.offset} | TS: ${r.Timestamp}] ${r.key ? r.key+': ' : ''}${atob(r.value || '')}`).join('\n');
                resultPre.innerText = formatted;
            } else {
                resultPre.innerText = "No messages available in this topic/group";
            }
        } else {
            resultPre.innerText = "Error consuming messages.";
        }
    } catch (err) {
        resultPre.innerText = err.name === 'TimeoutError' || err.name === 'AbortError' 
            ? "Network timeout while fetching." 
            : "Network error while fetching.";
    }
}

async function registerSchema(e) {
    e.preventDefault();
    const topic = document.getElementById('sTopic').value;
    const schema = document.getElementById('sSchema').value;
    const resultDiv = document.getElementById('schemaResult');

    try {
        const res = await fetch(`${API_BASE}/schemas`, {
            method: 'POST',
            headers: getHeaders(),
            body: JSON.stringify({ topic, schema })
        });
        if (res.ok) {
            resultDiv.innerHTML = `<div class="alert alert-success">Schema registered successfully</div>`;
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger">Error registering schema</div>`;
        }
    } catch (err) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Network error</div>`;
    }
}

async function fetchInsights() {
    const list = document.getElementById('insightsList');
    const cpuSpan = document.getElementById('aiCpuLoad');
    list.innerHTML = '<p class="text-muted">Analyzing...</p>';
    
    try {
        const res = await fetch(`${API_BASE}/ai/insights`, { headers: getHeaders() });
        if (res.ok) {
            const data = await res.json();
            cpuSpan.innerText = (data.cpu_load_percent || 0).toFixed(1);
            if (data.insights && data.insights.length > 0) {
                list.innerHTML = data.insights.map(i => {
                    let badge = 'bg-secondary';
                    if (i.level === 'CRITICAL') badge = 'bg-danger';
                    if (i.level === 'WARNING') badge = 'bg-warning text-dark';
                    if (i.level === 'INFO') badge = 'bg-info text-dark';
                    return `<div class="mb-2"><span class="badge ${badge}">${i.level}</span> ${i.message}</div>`;
                }).join('');
            } else {
                list.innerHTML = '<p class="text-muted">No insights available.</p>';
            }
        } else {
            list.innerHTML = '<p class="text-danger">Failed to fetch insights.</p>';
        }
    } catch (err) {
        list.innerHTML = '<p class="text-danger">Network error.</p>';
    }
}
