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
     * GỬI LOG LÊN DASHBOARD NEON
     */
    private void sendToDashboard(String type, String message, int clock) {
        Map<String, Object> logData = new HashMap<>();
        logData.put("type", type);
        logData.put("message", message);
        logData.put("lamportClock", clock);
        // Gửi về topic riêng của server hiện tại
        messagingTemplate.convertAndSend("/topic/logs/" + serverId, logData);
    }

    // 1. XỬ LÝ ĐẶT VÉ TỪ CLIENT
    @PostMapping("/bookings")
    public ResponseEntity<Map<String, Object>> handleClientBooking(@RequestBody Map<String, String> payload) {
        String flightId = payload.get("flightId");
        String userId = payload.get("userId");

        int currentTime = lamportClock.tick();

        // Bước 1: Log nhận lệnh
        sendToDashboard("CLIENT", ">>> BẮT ĐẦU: Nhận lệnh đặt vé từ " + userId, currentTime);

        try {
            // Bước 2: Log ghi DB riêng
            String dbName = "DB_" + serverId.replace("Cloud-Server-", "");
            sendToDashboard("DATABASE", "Ghi log giao dịch vào " + dbName + "...", currentTime);

            Booking newBooking = new Booking();
            newBooking.setPassengerName(userId);
            newBooking.setFlightId(flightId);
            newBooking.setLamportTimestamp(currentTime);
            newBooking.setServerId(serverId);
            bookingRepository.save(newBooking);

            sendToDashboard("DATABASE", "Lưu trữ nội bộ THÀNH CÔNG.", currentTime);
        } catch (Exception e) {
            sendToDashboard("ERROR", "Lỗi DB: " + e.getMessage(), currentTime);
        }

        // Bước 3: Phát tín hiệu đi qua các server khác
        broadcastSyncMessage(flightId, userId, currentTime);

        Map<String, Object> response = new HashMap<>();
        response.put("status", "SUCCESS");
        response.put("lamportClock", currentTime);
        return ResponseEntity.ok(response);
    }

    // 2. XỬ LÝ KHI NHẬN TÍN HIỆU ĐỒNG BỘ TỪ MÁY BẠN
    @PostMapping("/sync")
    public void handleSyncMessage(@RequestParam String serverOrigin,
            @RequestParam String flightId,
            @RequestParam String userId,
            @RequestParam int senderTime) {

        lamportClock.update(senderTime);
        int newTime = lamportClock.getTime();

        // Hiện log cực chi tiết khi có máy khác "ghé thăm"
        sendToDashboard("LAMPORT", "TIẾP NHẬN: " + serverOrigin + " yêu cầu đồng bộ.", newTime);

        try {
            Booking syncBooking = new Booking();
            syncBooking.setPassengerName(userId);
            syncBooking.setFlightId(flightId);
            syncBooking.setLamportTimestamp(senderTime);
            syncBooking.setServerId(serverOrigin);
            bookingRepository.save(syncBooking);

            sendToDashboard("DATABASE", "Đã chép dữ liệu từ " + serverOrigin + " vào DB cá nhân.", newTime);
        } catch (Exception e) {
            sendToDashboard("ERROR", "Lỗi đồng bộ: " + e.getMessage(), newTime);
        }
    }

    // 3. LOGIC GỬI TIN NHẮN ĐI TUẦN (Mấu chốt để hiện luồng chạy)
    private void broadcastSyncMessage(String flightId, String userId, int currentTime) {
        executor.submit(() -> {
            int nodeCount = 1;
            for (String peerUrl : peerServers) {
                if (peerUrl == null || peerUrl.trim().isEmpty())
                    continue;

                try {
                    // Log trạng thái bắt đầu đi tới Node tiếp theo
                    sendToDashboard("NETWORK", "Đang truyền dữ liệu tới Node " + nodeCount + " [" + peerUrl + "]",
                            currentTime);

                    String url = peerUrl + "/api/sync?serverOrigin=" + serverId
                            + "&flightId=" + flightId
                            + "&userId=" + userId
                            + "&senderTime=" + currentTime;

                    restTemplate.postForObject(url, null, String.class);

                    // Log khi Node đó đã nhận xong
                    sendToDashboard("SYNC", "Node " + nodeCount + " xác nhận: ĐỒNG BỘ XONG.", currentTime);
                } catch (Exception e) {
                    sendToDashboard("ERROR", "Node " + nodeCount + " KHÔNG PHẢN HỒI (Server chết hoặc sai URL)",
                            currentTime);
                }
                nodeCount++;

                // Nghỉ 400ms để log trên Dashboard chạy từ từ cho đẹp, không bị lặp dồn cục
                try {
                    Thread.sleep(400);
                } catch (InterruptedException ignored) {
                }
            }
            sendToDashboard("TRANSACTION", "=== KẾT THÚC CHU TRÌNH ĐỒNG BỘ HỆ PHÂN TÁN ===", currentTime);
        });
    }

    @GetMapping("/logs/private-view")
    public List<String> getLogs() {
        return logService.getAllLogs();
    }
}