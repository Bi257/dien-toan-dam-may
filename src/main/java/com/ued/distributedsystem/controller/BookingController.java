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
    private BookingRepository bookingRepository;

    @Value("#{'${peer.servers}'.split(',')}")
    private List<String> peerServers;

    @Value("${server.id:Cloud-Server-Duong}")
    private String serverId;

    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private final RestTemplate restTemplate = new RestTemplate();

    // HÀM GỬI LOG LÊN DASHBOARD (Đã fix để không bị lặp)
    private void sendToDashboard(String type, String message, int clock) {
        Map<String, Object> logData = new HashMap<>();
        logData.put("type", type);
        logData.put("message", message);
        logData.put("lamportClock", clock);
        // Quan trọng: Gửi về đúng kênh của Server ID này
        messagingTemplate.convertAndSend("/topic/logs/" + serverId, logData);
    }

    @PostMapping("/bookings")
    public ResponseEntity<Map<String, Object>> handleClientBooking(@RequestBody Map<String, String> payload) {
        String flightId = payload.get("flightId");
        String userId = payload.get("userId");
        int currentTime = lamportClock.tick();

        // 1. Log bắt đầu quy trình
        sendToDashboard("CLIENT", ">>> [BẮT ĐẦU] Nhận lệnh đặt vé từ: " + userId, currentTime);

        try {
            // 2. Ghi DB nội bộ trước
            Booking newBooking = new Booking();
            newBooking.setPassengerName(userId);
            newBooking.setFlightId(flightId);
            newBooking.setLamportTimestamp(currentTime);
            newBooking.setServerId(serverId);
            bookingRepository.save(newBooking);

            sendToDashboard("DATABASE", "Ghi log thành công vào MongoDB: DB_" + serverId.split("-")[2], currentTime);
        } catch (Exception e) {
            sendToDashboard("ERROR", "Lỗi DB: " + e.getMessage(), currentTime);
        }

        // 3. Gọi luồng đi tuần qua các server (Sequential Sync)
        broadcastSyncSequential(flightId, userId, currentTime);

        Map<String, Object> response = new HashMap<>();
        response.put("status", "SUCCESS");
        return ResponseEntity.ok(response);
    }

    private void broadcastSyncSequential(String flightId, String userId, int currentTime) {
        executor.submit(() -> {
            sendToDashboard("SYSTEM", "Khởi tạo đồng bộ tới " + peerServers.size() + " Nodes bạn bè...", currentTime);

            int count = 1;
            for (String peerUrl : peerServers) {
                if (peerUrl == null || peerUrl.trim().isEmpty())
                    continue;

                try {
                    // Hiện log "Đang đi tới..." giống hình 12009d
                    sendToDashboard("NETWORK", "Đang chuyển dữ liệu tới Node " + count + " [" + peerUrl + "]",
                            currentTime);

                    String url = peerUrl + "/api/sync?serverOrigin=" + serverId
                            + "&flightId=" + flightId
                            + "&userId=" + userId
                            + "&senderTime=" + currentTime;

                    restTemplate.postForObject(url, null, String.class);

                    // Hiện log xác nhận đã qua được Node đó
                    sendToDashboard("SYNC", "Node " + count + " phản hồi: OK (Đã đồng bộ)", currentTime);
                } catch (Exception e) {
                    sendToDashboard("ERROR", "Node " + count + " ngoại tuyến. Đang bỏ qua...", currentTime);
                }

                count++;
                // Nghỉ 600ms để log trên Dashboard chạy từ từ, không bị dính chùm
                try {
                    Thread.sleep(600);
                } catch (InterruptedException ignored) {
                }
            }
            sendToDashboard("SYSTEM", "=== HOÀN TẤT CHU TRÌNH ĐỒNG BỘ ===", currentTime);
        });
    }

    @PostMapping("/sync")
    public void handleSyncMessage(@RequestParam String serverOrigin, @RequestParam String flightId,
            @RequestParam String userId, @RequestParam int senderTime) {
        lamportClock.update(senderTime);
        int newTime = lamportClock.getTime();

        // Server nhận cũng báo log để thấy sự tương tác
        sendToDashboard("LAMPORT", "TIẾP NHẬN: " + serverOrigin + " đang yêu cầu đồng bộ (Clock: " + senderTime + ")",
                newTime);

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