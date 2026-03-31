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

    // HÀM GỬI LOG: Tui đã thêm kiểm tra để tránh gửi lặp
    private void sendToDashboard(String type, String message, int clock) {
        Map<String, Object> logData = new HashMap<>();
        logData.put("type", type);
        logData.put("message", message);
        logData.put("lamportClock", clock);
        logData.put("serverId", serverId);
        messagingTemplate.convertAndSend("/topic/logs/" + serverId, logData);
    }

    @PostMapping("/bookings")
    public ResponseEntity<Map<String, Object>> handleClientBooking(@RequestBody Map<String, String> payload) {
        String flightId = payload.get("flightId");
        String userId = payload.get("userId");
        int currentTime = lamportClock.tick();

        // 1. Chỉ log 1 dòng duy nhất khi nhận lệnh
        sendToDashboard("CLIENT", ">>> BẮT ĐẦU: Nhận lệnh đặt vé từ [" + userId + "]", currentTime);

        try {
            String dbName = "DB_" + serverId.replace("Cloud-Server-", "");
            Booking newBooking = new Booking();
            newBooking.setPassengerName(userId);
            newBooking.setFlightId(flightId);
            newBooking.setLamportTimestamp(currentTime);
            newBooking.setServerId(serverId);
            bookingRepository.save(newBooking);

            sendToDashboard("DATABASE", "Ghi vào " + dbName + " THÀNH CÔNG.", currentTime);
        } catch (Exception e) {
            sendToDashboard("ERROR", "Lỗi DB: " + e.getMessage(), currentTime);
        }

        // 2. Chạy luồng "Đi tuần qua các Server"
        broadcastSyncSequential(flightId, userId, currentTime);

        Map<String, Object> response = new HashMap<>();
        response.put("status", "SUCCESS");
        return ResponseEntity.ok(response);
    }

    // HÀM MỚI: Bắt buộc đi qua từng Server một cách tuần tự
    private void broadcastSyncSequential(String flightId, String userId, int currentTime) {
        executor.submit(() -> {
            sendToDashboard("SYSTEM", "Đang khởi tạo luồng đồng bộ tới " + peerServers.size() + " Nodes...",
                    currentTime);

            int count = 1;
            for (String peerUrl : peerServers) {
                if (peerUrl == null || peerUrl.trim().isEmpty())
                    continue;

                try {
                    // Log: Báo đang đi tới đâu
                    sendToDashboard("NETWORK", "Đang chuyển dữ liệu tới Node " + count + " [" + peerUrl + "]",
                            currentTime);

                    String url = peerUrl + "/api/sync?serverOrigin=" + serverId
                            + "&flightId=" + flightId
                            + "&userId=" + userId
                            + "&senderTime=" + currentTime;

                    restTemplate.postForObject(url, null, String.class);

                    // Log: Báo Node đó đã nhận
                    sendToDashboard("SYNC", "Node " + count + " ĐÃ NHẬN (Đồng bộ Clock OK)", currentTime);
                } catch (Exception e) {
                    sendToDashboard("ERROR", "Node " + count + " ngoại tuyến. Bỏ qua...", currentTime);
                }

                count++;
                // Nghỉ 500ms để log không bị dính chùm và lặp
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ignored) {
                }
            }
            sendToDashboard("SYSTEM", "=== HOÀN TẤT CHU TRÌNH HỆ PHÂN TÁN ===", currentTime);
        });
    }

    @PostMapping("/sync")
    public void handleSyncMessage(@RequestParam String serverOrigin,
            @RequestParam String flightId,
            @RequestParam String userId,
            @RequestParam int senderTime) {
        lamportClock.update(senderTime);
        int newTime = lamportClock.getTime();

        // Node nhận chỉ log đúng dòng này để không bị loạn Dashboard
        sendToDashboard("LAMPORT", "TIẾP NHẬN: " + serverOrigin + " đang đồng bộ dữ liệu.", newTime);

        try {
            Booking syncBooking = new Booking();
            syncBooking.setPassengerName(userId);
            syncBooking.setFlightId(flightId);
            syncBooking.setLamportTimestamp(senderTime);
            syncBooking.setServerId(serverOrigin);
            bookingRepository.save(syncBooking);
        } catch (Exception ignored) {
        }
    }
}