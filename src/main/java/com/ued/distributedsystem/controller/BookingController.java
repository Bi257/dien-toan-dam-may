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
    private BookingRepository bookingRepository;

    @Value("#{'${peer.servers}'.split(',')}")
    private List<String> peerServers;

    @Value("${server.id:Cloud-Server-Duong}")
    private String serverId;

    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private final RestTemplate restTemplate = new RestTemplate();

    // GỬI LOG (Chỉ dùng WebSocket để tránh lặp)
    private void sendToDashboard(String type, String message, int clock) {
        Map<String, Object> logData = new HashMap<>();
        logData.put("type", type);
        logData.put("message", message);
        logData.put("lamportClock", clock);
        messagingTemplate.convertAndSend("/topic/logs/" + serverId, logData);
    }

    @PostMapping("/bookings")
    public ResponseEntity<Map<String, Object>> handleClientBooking(@RequestBody Map<String, String> payload) {
        String flightId = payload.getOrDefault("flightId", "FL-999");
        String userId = payload.getOrDefault("userId", "Khách-Ẩn-Danh");
        int currentTime = lamportClock.tick();

        // LOG 1: Nhận lệnh
        sendToDashboard("CLIENT", ">>> [BẮT ĐẦU] Nhận lệnh đặt vé từ Client: " + userId, currentTime);

        try {
            // LOG 2: Ghi MongoDB
            Booking b = new Booking();
            b.setPassengerName(userId);
            b.setFlightId(flightId);
            b.setLamportTimestamp(currentTime);
            b.setServerId(serverId);
            bookingRepository.save(b);
            sendToDashboard("DATABASE", "Ghi log thành công vào MongoDB: Cluster0", currentTime);
        } catch (Exception e) {
            sendToDashboard("ERROR", "Lỗi DB: " + e.getMessage(), currentTime);
        }

        // LOG 3: Bắt đầu đi tuần (Tuần tự)
        broadcastSyncSequential(flightId, userId, currentTime);

        Map<String, Object> response = new HashMap<>();
        response.put("status", "SUCCESS");
        return ResponseEntity.ok(response);
    }

    private void broadcastSyncSequential(String flightId, String userId, int currentTime) {
        executor.submit(() -> {
            int nodeIdx = 1;
            for (String peerUrl : peerServers) {
                if (peerUrl == null || peerUrl.trim().isEmpty())
                    continue;
                try {
                    // LOG: Đang ghé thăm node bạn
                    sendToDashboard("NETWORK", "--- Đang truyền tin tới Node " + nodeIdx + " (" + peerUrl + ")",
                            currentTime);

                    String url = peerUrl + "/api/sync?serverOrigin=" + serverId
                            + "&flightId=" + flightId + "&userId=" + userId + "&senderTime=" + currentTime;

                    restTemplate.postForObject(url, null, String.class);

                    // LOG: Node bạn xác nhận
                    sendToDashboard("SYNC", "Node " + nodeIdx + " xác nhận: ĐÃ NHẬN", currentTime);

                    // NGHỈ ĐỂ THẤY LUỒNG CHẠY
                    Thread.sleep(600);
                } catch (Exception e) {
                    sendToDashboard("ERROR", "Node " + nodeIdx + " offline.", currentTime);
                }
                nodeIdx++;
            }
            sendToDashboard("SYSTEM", "=== HOÀN TẤT CHU TRÌNH ĐỒNG BỘ ===", currentTime);
        });
    }

    @PostMapping("/sync")
    public void handleSyncMessage(@RequestParam String serverOrigin, @RequestParam String flightId,
            @RequestParam String userId, @RequestParam int senderTime) {
        lamportClock.update(senderTime);
        sendToDashboard("LAMPORT", "TIẾP NHẬN: Yêu cầu đồng bộ từ " + serverOrigin, lamportClock.getTime());
    }
}