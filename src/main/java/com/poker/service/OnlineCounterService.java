package com.poker.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Periodic re-broadcast of the online counter so that clients that missed an update converge.
 * The count itself is owned by {@link WebSocketEventListener}; publishing a second, independently
 * derived number here would make the value flicker between two sources of truth.
 */
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
