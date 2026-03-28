package com.ued.distributedsystem.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ued.distributedsystem.model.LamportClock;
import com.ued.distributedsystem.model.Booking;
import com.ued.distributedsystem.service.LogService;
import com.ued.distributedsystem.repository.BookingRepository;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class BookingController {

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    @Autowired
    private LamportClock lamportClock;

    @Autowired
    private LogService logService;

    @Autowired
    private BookingRepository bookingRepository;

    @Value("#{'${peer.servers}'.split(',')}")
    private List<String> peerServers;

    @Value("${server.id:Cloud-Server-Duong}")
    private String serverId;

    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private final RestTemplate restTemplate = new RestTemplate();

    /**
     * HÀM GỬI LOG LÊN DASHBOARD NEON QUA WEBSOCKET
     */
    private void sendToDashboard(String type, String message, int clock) {
        Map<String, Object> logData = new HashMap<>();
        logData.put("type", type);
        logData.put("message", message);
        logData.put("lamportClock", clock);
        // Gửi đến topic mà Dashboard đang nghe (Cloud-Server-Duong)
        messagingTemplate.convertAndSend("/topic/logs/" + serverId, logData);
    }

    // 1. XỬ LÝ ĐẶT VÉ TỪ CLIENT
    @PostMapping("/booking")
    public ResponseEntity<String> handleClientBooking(@RequestParam String flightId, @RequestParam String userId) {

        // A. Tăng đồng hồ Lamport nội bộ
        int currentTime = lamportClock.tick();

        // B. Gửi Log lên Dashboard ngay lập tức để hiện tia sáng Neon
        sendToDashboard("BOOKING", "Khách " + userId + " đặt vé chuyến " + flightId, currentTime);
        logService.addLog("BOOKING", "Nhận yêu cầu từ " + userId + ". Flight: " + flightId, currentTime);

        try {
            // C. Lưu vào MongoDB Atlas
            Booking newBooking = new Booking();
            newBooking.setPassengerName(userId);
            newBooking.setFlightId(flightId);
            newBooking.setLamportTimestamp(currentTime);
            newBooking.setServerId(serverId);

            bookingRepository.save(newBooking);
            logService.addLog("INFO", "Đã lưu vé lên MongoDB Atlas", currentTime);
        } catch (Exception e) {
            sendToDashboard("ERROR", "Lỗi DB: " + e.getMessage(), currentTime);
            logService.addLog("ERROR", "Lỗi lưu DB: " + e.getMessage(), currentTime);
        }

        // D. Phát tín hiệu đồng bộ cho các server còn lại
        broadcastSyncMessage(flightId, userId, currentTime);

        return ResponseEntity.ok("Đặt vé thành công! Timestamp: " + currentTime);
    }

    // 2. XỬ LÝ KHI NHẬN ĐƯỢC TIN ĐỒNG BỘ TỪ SERVER BẠN
    @PostMapping("/sync")
    public void handleSyncMessage(@RequestParam String serverOrigin,
            @RequestParam String flightId,
            @RequestParam String userId,
            @RequestParam int senderTime) {

        // Cập nhật Lamport Clock theo thuật toán: max(local, remote) + 1
        lamportClock.update(senderTime);
        int newTime = lamportClock.getTime();

        // Hiện tia sáng Sync màu tím trên Dashboard
        sendToDashboard("SYNC", "Nhận đồng bộ từ " + serverOrigin, newTime);
        logService.addLog("SYNC", "Đồng bộ từ " + serverOrigin + " cho khách: " + userId, newTime);
    }

    @GetMapping("/logs/private-view")
    public List<String> getLogs() {
        return logService.getAllLogs();
    }

    // 3. HÀM PHÁT TÁN BẢN TIN (BROADCAST)
    private void broadcastSyncMessage(String flightId, String userId, int currentTime) {
        for (String peerUrl : peerServers) {
            if (peerUrl == null || peerUrl.trim().isEmpty())
                continue;

            executor.submit(() -> {
                try {
                    String url = peerUrl + "/api/sync?serverOrigin=" + serverId
                            + "&flightId=" + flightId
                            + "&userId=" + userId
                            + "&senderTime=" + currentTime;

                    restTemplate.postForObject(url, null, String.class);
                    logService.addLog("INFO", "Đã gửi đồng bộ tới " + peerUrl, currentTime);
                } catch (Exception e) {
                    // Nếu máy bạn offline, hiện lỗi lên Dashboard
                    sendToDashboard("ERROR", "Peer Offline: " + peerUrl, currentTime);
                    logService.addLog("ERROR", "Máy bạn offline: " + peerUrl, currentTime);
                }
            });
        }
    }
}