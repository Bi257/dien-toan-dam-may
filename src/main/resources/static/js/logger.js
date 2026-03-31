let stompClient = null;
let currentSelectedServer = "Cloud-Server-Duong"; // Mặc định

// Trong logger.js
function connect() {
    const socket = new SockJS('/ws');
    stompClient = Stomp.over(socket);
    stompClient.debug = null;

    stompClient.connect({}, function () {
        // Đăng ký nhận log tổng hợp từ tất cả các Server
        stompClient.subscribe('/topic/logs/all', function (res) {
            const data = JSON.parse(res.body);
            const time = new Date().toLocaleTimeString('en-GB', { hour12: false });
            
            // Dữ liệu thật từ các máy sẽ có nodeId khác nhau
            const logString = `[${time}] [${data.type}] [Node: ${data.nodeId}] ${data.message} | Clock: ${data.lamportClock}`;

            // Hiện ở log tổng hợp (Dưới cùng)
            appendLog('global-log-display', logString);

            // Hiện ở log chi tiết (Nếu đang chọn đúng server đó trên Dropdown)
            if (data.nodeId === currentSelectedServer) {
                appendLog('specific-log-display', logString);
                // Làm nhấp nháy đèn trên sơ đồ vòng
                triggerNodeAnimation(data.nodeId);
            }
        });
    });
}

function appendLog(elementId, text) {
    const container = document.getElementById(elementId);
    const div = document.createElement('div');
    div.innerText = text;
    container.appendChild(div);
    container.scrollTop = container.scrollHeight; // Auto-scroll
}

function changeServerView() {
    // Lấy giá trị Server đang được chọn trong dropdown
    currentSelectedServer = document.getElementById('server-select').value;
    
    // Đổi màu cái cục tròn trong Sơ đồ: Cục nào đang chọn thì tô Cam, còn lại xanh lá
    document.querySelectorAll('.node').forEach(n => n.classList.remove('active'));
    
    if (currentSelectedServer.includes('Duong')) document.getElementById('node-master').classList.add('active');
    else if (currentSelectedServer.includes('Tram')) document.getElementById('node-tram').classList.add('active');
    else document.getElementById('node-chung').classList.add('active');

    // Xóa ô log chi tiết cũ để xem log mới của server vừa chọn
    document.getElementById('specific-log-display').innerHTML = '';
}

function clearLogs() {
    document.getElementById('specific-log-display').innerHTML = '';
    document.getElementById('global-log-display').innerHTML = '';
}

document.addEventListener('DOMContentLoaded', connect);