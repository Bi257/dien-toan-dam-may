let stompClient = null;
// Lấy Node ID từ giao diện để biết đang ở máy Dương hay máy Trâm
const nodeId = document.getElementById('node-id').innerText.trim() || 'Cloud-Server-Duong';

function connectWebSocket() {
    const socket = new SockJS('/ws');
    stompClient = Stomp.over(socket);
    stompClient.debug = null; // Tắt debug cho đỡ rối console

    stompClient.connect({}, function (frame) {
        console.log('Connected: ' + frame);
        
        // Đăng ký kênh nhận log duy nhất
        stompClient.subscribe('/topic/logs/' + nodeId, function (sdkEvent) {
            const logData = JSON.parse(sdkEvent.body);
            renderLogToScreen(logData);
        });
    });
}

function renderLogToScreen(log) {
    const logDisplay = document.getElementById('log-display');
    const clockDisplay = document.getElementById('clock-val');
    
    // Cập nhật đồng hồ Lamport trung tâm
    if (log.lamportClock !== undefined) {
        clockDisplay.innerText = log.lamportClock;
    }

    // Tạo dòng log mới
    const div = document.createElement('div');
    div.className = `log-entry log-${log.type}`;
    
    const now = new Date();
    const timestamp = now.toLocaleTimeString('en-GB', { hour12: false });

    // Cấu trúc log giống hệt hình 12009d
    div.innerHTML = `<span class="log-time">[${timestamp}]</span> 
                     <span class="log-type">[${log.type}]</span> 
                     <span class="log-msg">${log.message}</span>`;
    
    logDisplay.appendChild(div);
    
    // Luôn cuộn xuống dòng mới nhất
    logDisplay.scrollTop = logDisplay.scrollHeight;
}

// Chạy ngay khi trang web sẵn sàng
document.addEventListener('DOMContentLoaded', connectWebSocket);