let stompClient = null;
let currentSelectedServer = "Cloud-Server-Duong"; // Mặc định

function connect() {
    const socket = new SockJS('/ws');
    stompClient = Stomp.over(socket);
    stompClient.debug = null; // Tắt log rác trong console

    stompClient.connect({}, function () {
        // Lắng nghe TẤT CẢ log từ Backend bắn về topic này
        stompClient.subscribe('/topic/logs/Cloud-Server-Duong', function (res) {
            const data = JSON.parse(res.body);
            const time = new Date().toLocaleTimeString('en-GB', { hour12: false });
            
            // Format log gộp thành 1 chuỗi dài giống hình 14d598.jpg: 
            // [TIME] [NODE] Noi dung | Sequence=...
            const logString = `[${time}] [${data.type}] [${data.nodeId || 'Master'}] ${data.message} | LamportClock=${data.lamportClock}`;

            // 1. Luôn in vào Dòng Log Tổng Hợp (Ô bự phía dưới)
            appendLog('global-log-display', logString);

            // 2. Chỉ in vào Log Chi Tiết (Ô nhỏ góc phải) nếu khớp với lựa chọn Dropdown
            if ((data.nodeId || 'Cloud-Server-Duong') === currentSelectedServer) {
                appendLog('specific-log-display', logString);
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

document.addEventListener('DOMContentLoaded', connect);git add .
git commit -m "cap nhat"
git push origin main