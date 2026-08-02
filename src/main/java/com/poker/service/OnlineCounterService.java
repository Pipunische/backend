package com.poker.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class OnlineCounterService {

    private final WebSocketEventListener webSocketEventListener;

    @Scheduled(fixedRate = 10000)
    public void broadcastOnlineCount() {
        webSocketEventListener.broadcastOnlineCount();
    }
}
