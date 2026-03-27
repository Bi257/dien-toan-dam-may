package com.ued.distributedsystem.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ued.distributedsystem.model.LamportClock;
import com.ued.distributedsystem.model.Booking; // Đảm bảo đã có Model này
import com.ued.distributedsystem.service.LogService;
import com.ued.distributedsystem.repository.BookingRepository; // Đảm bảo đã có Repo này

@RestController
@RequestMapping("/api")
public class BookingController {

    @Autowired
    private LamportClock lamportClock;

    @Autowired
    private LogService logService;

    @Autowired
    private BookingRepository bookingRepository;

    @Value("#{'${peer.servers}'.split(',')}")
    private List<String> peerServers;

    @Value("${server.id}")
    private String serverId;

    private final ExecutorService executor = Executors.newFixedThreadPool(10); // Tăng pool để xử lý 5 máy
    private final RestTemplate restTemplate = new RestTemplate();

    // ĐÂY LÀ HÀM DUY NHẤT XỬ LÝ ĐẶT VÉ
    @PostMapping("/booking")
    public ResponseEntity<String> handleClientBooking(@RequestParam String flightId, @RequestParam String userId) {
        // 1. Tăng đồng hồ Lamport nội bộ
        int currentTime = lamportClock.tick();
        logService.addLog("BOOKING", "Nhận yêu cầu từ " + userId + ". Flight: " + flightId, currentTime);

        try {
            // 2. Lưu vào MongoDB Atlas
            Booking newBooking = new Booking();
            newBooking.setPassengerName(userId);
            newBooking.setFlightId(flightId);
            newBooking.setLamportTimestamp(currentTime);
            newBooking.setServerId(serverId);

            bookingRepository.save(newBooking);
            logService.addLog("INFO", "Đã lưu vé của " + userId + " lên MongoDB Atlas", currentTime);
        } catch (Exception e) {
            logService.addLog("ERROR", "Lỗi lưu DB: " + e.getMessage(), currentTime);
        }

        // 3. Phát tín hiệu đồng bộ cho các server còn lại
        broadcastSyncMessage(flightId, userId, currentTime);

        return ResponseEntity.ok("Đặt vé thành công! Timestamp: " + currentTime);
    }

    @PostMapping("/sync")
    public void handleSyncMessage(@RequestParam String serverOrigin,
            @RequestParam String flightId,
            @RequestParam String userId, // Thêm userId vào để log cho rõ
            @RequestParam int senderTime) {
        lamportClock.update(senderTime);
        int newTime = lamportClock.getTime();
        logService.addLog("SYNC", "Đồng bộ từ " + serverOrigin + " cho khách: " + userId, newTime);
    }

    @GetMapping("/logs/private-view")
    public List<String> getLogs() {
        return logService.getAllLogs();
    }

    private void broadcastSyncMessage(String flightId, String userId, int currentTime) {
        for (String peerUrl : peerServers) {
            if (peerUrl == null || peerUrl.isEmpty())
                continue;

            executor.submit(() -> {
                try {
                    // Gửi thêm userId sang để các server kia biết ai đặt
                    String url = peerUrl + "/api/sync?serverOrigin=" + serverId
                            + "&flightId=" + flightId
                            + "&userId=" + userId
                            + "&senderTime=" + currentTime;
                    restTemplate.postForObject(url, null, String.class);
                    logService.addLog("INFO", "Đã gửi đồng bộ tới " + peerUrl, lamportClock.getTime());
                } catch (Exception e) {
                    logService.addLog("ERROR", "Máy bạn offline: " + peerUrl, lamportClock.getTime());
                }
            });
        }
    }
}