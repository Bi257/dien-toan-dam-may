package com.ued.distributedsystem.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.util.*;
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

    private final RestTemplate restTemplate = new RestTemplate();

    // Trong BookingController.java
    // Trong BookingController.java

    private void sendToDashboard(String type, String message, int clock) {
        Map<String, Object> logData = new HashMap<>();
        logData.put("type", type);
        logData.put("message", message);
        logData.put("lamportClock", clock);
        logData.put("nodeId", serverId); // serverId lấy từ @Value("${server.id}")
        logData.put("timestamp", System.currentTimeMillis());

        // 1. Gửi vào Topic chung (Để xem toàn bộ Cluster)
        messagingTemplate.convertAndSend("/topic/logs/all", logData);

        // 2. Gửi vào Topic riêng của Server này (Để xem riêng lẻ)
        // Ví dụ: /topic/logs/Cloud-Server-Duong
        messagingTemplate.convertAndSend("/topic/logs/" + serverId, logData);
    }

    @PostMapping("/bookings")
    public ResponseEntity<?> handleBooking(@RequestBody Map<String, String> payload) {
        String user = payload.getOrDefault("userId", "Khách-Hàng");
        String flight = payload.getOrDefault("flightId", "FL-999");
        int time = lamportClock.tick();

        // LOG 1: Nhận lệnh từ Client
        sendToDashboard("CLIENT", ">>> [BẮT ĐẦU] Nhận lệnh đặt vé từ: " + user, time);

        try {
            // LOG 2: Ghi MongoDB (Dùng SET để tránh lỗi Constructor)
            Booking b = new Booking();
            b.setPassengerName(user);
            b.setFlightId(flight);
            b.setLamportTimestamp(time);
            b.setServerId(serverId);
            bookingRepository.save(b);
            sendToDashboard("DATABASE", "Đã lưu giao dịch vào MongoDB Cluster0", time);
        } catch (Exception e) {
            sendToDashboard("ERROR", "Lỗi DB: " + e.getMessage(), time);
        }

        // LOG 3: Chạy luồng đi tuần qua từng Server bạn
        Executors.newSingleThreadExecutor().submit(() -> {
            int idx = 1;
            for (String peer : peerServers) {
                if (peer == null || peer.trim().isEmpty())
                    continue;
                try {
                    // Thông báo chuẩn bị gửi
                    sendToDashboard("NETWORK", "--- Đang chuyển dữ liệu tới Node " + idx + " [" + peer + "]", time);

                    String url = peer + "/api/sync?serverOrigin=" + serverId
                            + "&flightId=" + flight + "&userId=" + user + "&senderTime=" + time;

                    restTemplate.postForObject(url, null, String.class);

                    // Thông báo đã gửi xong
                    sendToDashboard("SYNC", "Node " + idx + " phản hồi: OK (Đã đồng bộ)", time);

                    // NGHỈ ĐỂ THẤY LUỒNG CHẠY TRÊN DASHBOARD
                    Thread.sleep(800);
                } catch (Exception e) {
                    sendToDashboard("ERROR", "Node " + idx + " KHÔNG PHẢN HỒI (Offline)", time);
                }
                idx++;
            }
            sendToDashboard("SYSTEM", "=== KẾT THÚC QUY TRÌNH ĐỒNG BỘ HỆ PHÂN TÁN ===", time);
        });

        return ResponseEntity.ok(Map.of("status", "SUCCESS"));
    }

    @PostMapping("/sync")
    public void handleSync(@RequestParam String serverOrigin, @RequestParam int senderTime) {
        lamportClock.update(senderTime);
        sendToDashboard("LAMPORT", "TIẾP NHẬN: Yêu cầu đồng bộ từ [" + serverOrigin + "]", lamportClock.getTime());
    }
}