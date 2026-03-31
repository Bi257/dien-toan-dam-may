let currentFilter = 'ALL';
let stompClient = null;
const serverId = document.getElementById('node-id').innerText.trim() || 'Cloud-Server-Duong';

function connectWebSocket() {
    const socket = new SockJS('/ws');
    stompClient = Stomp.over(socket);
    stompClient.debug = null; // Tắt log debug của stomp cho sạch console

    stompClient.connect({}, function (frame) {
        console.log('Connected to WebSocket: ' + frame);
        
        // Subscribe đúng topic của Server này
        stompClient.subscribe('/topic/logs/' + serverId, function (logUpdate) {
            const logData = JSON.parse(logUpdate.body);
            displaySingleLog(logData);
        });
    }, function(error) {
        console.log("WebSocket error, retrying in 5s...");
        setTimeout(connectWebSocket, 5000);
    });
}

function displaySingleLog(log) {
    const logDisplay = document.getElementById('log-display');
    const clockDisplay = document.getElementById('clock-val');

    // Cập nhật đồng hồ Lamport trên Dashboard
    if (log.lamportClock) {
        clockDisplay.innerText = log.lamportClock;
    }

    // Áp dụng bộ lọc (Filter)
    if (currentFilter === 'ALL' || log.type === currentFilter) {
        const div = document.createElement('div');
        div.className = `log-entry log-${log.type} new-log-anim`; // Thêm class hiệu ứng nếu có
        
        // Định dạng dòng log giống như hình mẫu 12009d
        const time = new Date().toLocaleTimeString('en-GB', { hour12: false });
        div.innerHTML = `<span class="log-time">[${time}]</span> 
                         <span class="log-type">[${log.type}]</span> 
                         <span class="log-msg">${log.message}</span>`;
        
        logDisplay.appendChild(div);
        
        // Tự động cuộn xuống cuối
        logDisplay.scrollTop = logDisplay.scrollHeight;

        // Giới hạn số lượng dòng hiển thị để tránh lag (ví dụ giữ 50 dòng mới nhất)
        if (logDisplay.childNodes.length > 50) {
            logDisplay.removeChild(logDisplay.firstChild);
        }
    }
}

function filterLogs(filter) {
    currentFilter = filter;
    // Xóa màn hình khi đổi filter để lọc lại từ đầu nếu muốn, hoặc cứ giữ nguyên
    document.querySelectorAll('.btn-filter').forEach(btn => {
        btn.classList.toggle('active', btn.innerText === filter);
    });
}

// Khởi tạo kết nối khi load trang
document.addEventListener('DOMContentLoaded', () => {
    connectWebSocket();
});