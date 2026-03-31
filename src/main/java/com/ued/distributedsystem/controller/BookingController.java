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
        messagingTemplate.convertAndSend("/topic/logs/" + serverId, logData);
    }

    // 1. XỬ LÝ ĐẶT VÉ TỪ CLIENT
    @PostMapping("/bookings")
    public ResponseEntity<Map<String, Object>> handleClientBooking(@RequestBody Map<String, String> payload) {
        String flightId = payload.get("flightId");
        String userId = payload.get("userId");

        int currentTime = lamportClock.tick();

        // LOG 1: Nhận lệnh từ App
        sendToDashboard("CLIENT", "Nhận lệnh ĐẶT VÉ [" + flightId + "] từ Client: " + userId, currentTime);
        logService.addLog("CLIENT", "Request từ " + userId, currentTime);

        try {
            // LOG 2: Ghi vào DB nội bộ
            String dbName = "DB_" + serverId.replace("Cloud-Server-", "");
            sendToDashboard("DATABASE", "Đang kết nối và ghi vào: " + dbName, currentTime);

            Booking newBooking = new Booking();
            newBooking.setPassengerName(userId);
            newBooking.setFlightId(flightId);
            newBooking.setLamportTimestamp(currentTime);
            newBooking.setServerId(serverId);

            bookingRepository.save(newBooking);
            sendToDashboard("DATABASE", "Ghi transaction THÀNH CÔNG vào " + dbName, currentTime);
        } catch (Exception e) {
            sendToDashboard("ERROR", "Lỗi ghi DB: " + e.getMessage(), currentTime);
        }

        // LOG 3: Bắt đầu giai đoạn đồng bộ
        sendToDashboard("TRANSACTION", "BẮT ĐẦU quy trình đồng bộ liên Server...", currentTime);
        broadcastSyncMessage(flightId, userId, currentTime);

        Map<String, Object> response = new HashMap<>();
        response.put("message", "Thành công");
        response.put("lamportClock", currentTime);

        return ResponseEntity.ok(response);
    }

    // 2. XỬ LÝ ĐỒNG BỘ TỪ SERVER BẠN
    @PostMapping("/sync")
    public void handleSyncMessage(@RequestParam String serverOrigin,
            @RequestParam String flightId,
            @RequestParam String userId,
            @RequestParam int senderTime) {

        lamportClock.update(senderTime);
        int newTime = lamportClock.getTime();

        // LOG: Hiện rõ server nào gửi và quá trình lưu DB đồng bộ
        sendToDashboard("LAMPORT", "Nhận tín hiệu SYNC từ " + serverOrigin + " (Clock: " + senderTime + ")", newTime);

        try {
            Booking syncBooking = new Booking();
            syncBooking.setPassengerName(userId);
            syncBooking.setFlightId(flightId);
            syncBooking.setLamportTimestamp(senderTime);
            syncBooking.setServerId(serverOrigin);
            bookingRepository.save(syncBooking);

            sendToDashboard("DATABASE", "Đã đồng bộ vé [" + flightId + "] vào DB nội bộ từ " + serverOrigin, newTime);
        } catch (Exception e) {
            sendToDashboard("ERROR", "Lỗi đồng bộ DB: " + e.getMessage(), newTime);
        }
    }

    @GetMapping("/logs/private-view")
    public List<String> getLogs() {
        return logService.getAllLogs();
    }

    // 3. BROADCAST (Gửi đi kèm Log từng Node)
    private void broadcastSyncMessage(String flightId, String userId, int currentTime) {
        int nodeIndex = 1;
        for (String peerUrl : peerServers) {
            if (peerUrl == null || peerUrl.trim().isEmpty())
                continue;

            final int index = nodeIndex++;
            executor.submit(() -> {
                try {
                    // LOG: Trạng thái gửi
                    sendToDashboard("NETWORK", "Đang truyền tin tới Node " + index + " (" + peerUrl + ")", currentTime);

                    String url = peerUrl + "/api/sync?serverOrigin=" + serverId
                            + "&flightId=" + flightId
                            + "&userId=" + userId
                            + "&senderTime=" + currentTime;

                    restTemplate.postForObject(url, null, String.class);

                    // LOG: Xác nhận phản hồi
                    sendToDashboard("SYNC", "Node " + index + " phản hồi: ĐÃ NHẬN", currentTime);
                } catch (Exception e) {
                    sendToDashboard("ERROR", "Node " + index + " KHÔNG PHẢN HỒI (Offline)", currentTime);
                }
            });
        }
    }
}