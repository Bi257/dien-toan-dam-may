let stompClient = null;
const myNodeId = document.getElementById('node-id').innerText.trim();

function connect() {
    const socket = new SockJS('/ws');
    stompClient = Stomp.over(socket);
    stompClient.debug = null;

    stompClient.connect({}, function (frame) {
        stompClient.subscribe('/topic/logs/' + myNodeId, function (response) {
            const log = JSON.parse(response.body);
            appendLog(log);
            updateNodeVisuals(log);
        });
    });
}

function appendLog(log) {
    const container = document.getElementById('log-display');
    const row = document.createElement('div');
    row.className = `log-row log-${log.type}`;
    
    const time = new Date().toLocaleTimeString('en-GB', { hour12: false });
    
    // Chia log thành 3 cột rõ ràng: TIME | TYPE | MESSAGE
    row.innerHTML = `
        <div class="col-time">[${time}]</div>
        <div class="col-type">[${log.type}]</div>
        <div class="col-msg">${log.message}</div>
    `;
    
    container.appendChild(row);
    container.scrollTop = container.scrollHeight;
    
    if(log.lamportClock) document.getElementById('clock-val').innerText = log.lamportClock;
}

function updateNodeVisuals(log) {
    // Hiệu ứng chớp đèn khi dữ liệu đi qua từng máy
    if (log.message.includes("Node 1")) {
        const dot = document.getElementById('dot-1');
        dot.style.background = (log.type === 'SYNC') ? '#00ff00' : '#ffcc00';
    }
}

document.addEventListener('DOMContentLoaded', connect);