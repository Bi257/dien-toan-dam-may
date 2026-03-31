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

    private void sendToDashboard(String type, String message, int clock) {
        Map<String, Object> logData = new HashMap<>();
        logData.put("type", type);
        logData.put("message", message);
        logData.put("lamportClock", clock);
        logData.put("nodeId", serverId);
        messagingTemplate.convertAndSend("/topic/logs/" + serverId, logData);
    }

    @PostMapping("/bookings")
    public ResponseEntity<?> handleBooking(@RequestBody Map<String, String> payload) {
        String user = payload.getOrDefault("userId", "User-Unknown");
        int time = lamportClock.tick();

        sendToDashboard("CLIENT", ">>> [BẮT ĐẦU] Nhận yêu cầu đặt vé từ: " + user, time);

        // Lưu DB nội bộ
        try {
            Booking b = new Booking(user, payload.get("flightId"), time, serverId);
            bookingRepository.save(b);
            sendToDashboard("DATABASE", "Đã ghi nhận giao dịch vào MongoDB Cluster0", time);
        } catch (Exception e) {
            sendToDashboard("ERROR", "Lỗi DB: " + e.getMessage(), time);
        }

        // Kích hoạt luồng đi tuần tự qua các Node
        Executors.newSingleThreadExecutor().submit(() -> {
            int idx = 1;
            for (String peer : peerServers) {
                if (peer == null || peer.isEmpty())
                    continue;
                try {
                    sendToDashboard("NETWORK", "Đang truyền gói tin tới Node " + idx + " [" + peer + "]", time);
                    restTemplate.postForObject(
                            peer + "/api/sync?serverOrigin=" + serverId + "&userId=" + user + "&senderTime=" + time,
                            null, String.class);
                    sendToDashboard("SYNC", "Node " + idx + " xác nhận: ĐỒNG BỘ HOÀN TẤT", time);
                    Thread.sleep(700); // Nghỉ để Dashboard kịp hiển thị luồng đi
                } catch (Exception e) {
                    sendToDashboard("ERROR", "Node " + idx + " mất kết nối.", time);
                }
                idx++;
            }
            sendToDashboard("SYSTEM", "=== KẾT THÚC CHU TRÌNH HỆ PHÂN TÁN ===", time);
        });

        return ResponseEntity.ok(Map.of("status", "SUCCESS"));
    }

    @PostMapping("/sync")
    public void handleSync(@RequestParam String serverOrigin, @RequestParam int senderTime) {
        lamportClock.update(senderTime);
        sendToDashboard("LAMPORT", "TIẾP NHẬN: Yêu cầu đồng bộ từ " + serverOrigin, lamportClock.getTime());
    }
}