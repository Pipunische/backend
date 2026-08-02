package com.poker.service;

import com.poker.config.StompAuthChannelInterceptor;
import com.poker.dto.TableDTO;
import com.poker.dto.events.LobbySnapshotDTO;
import com.poker.dto.events.OnlineUpdateDTO;
import com.poker.dto.events.TableDetailsDTO;
import com.poker.model.Table;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.broker.AbstractBrokerMessageHandler;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ExecutorChannelInterceptor;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.SessionConnectedEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class WebSocketEventListener implements ExecutorChannelInterceptor {

    private static final String LOBBY_DESTINATION = "/topic/lobby";
    private static final String TABLE_DESTINATION_PREFIX = "/topic/table/";

    private final TableManager tableManager;
    private final SimpMessagingTemplate messagingTemplate;

    private final Map<String, Set<String>> sessionsByUser = new ConcurrentHashMap<>();

    @EventListener
    public void handleWebSocketConnectListener(SessionConnectedEvent event) {
        StompHeaderAccessor headerAccessor = StompHeaderAccessor.wrap(event.getMessage());
        String userId = resolveUserId(headerAccessor);
        String sessionId = headerAccessor.getSessionId();

        if (userId == null || sessionId == null) {
            return;
        }

        boolean firstSession = registerSession(userId, sessionId);
        tableManager.cancelDisconnectTask(userId);

        if (firstSession) {
            broadcastOnlineCount();
        }
    }

    @EventListener
    public void handleWebSocketDisconnectListener(SessionDisconnectEvent event) {
        StompHeaderAccessor headerAccessor = StompHeaderAccessor.wrap(event.getMessage());
        String userId = resolveUserId(headerAccessor);
        String sessionId = headerAccessor.getSessionId();

        if (userId == null || sessionId == null) {
            return;
        }

        boolean wasLastSession = unregisterSession(userId, sessionId);
        if (!wasLastSession) {
            log.debug("Session {} of user {} disconnected, other sessions are still open.", sessionId, userId);
            return;
        }

        log.info("Last WebSocket session closed for user {}. Scheduling grace period kick...", userId);
        tableManager.scheduleDisconnectKick(userId);
        broadcastOnlineCount();
    }

    // Not a SessionSubscribeEvent listener: that event fires before the broker registers the
    // subscription, and the simple broker drops messages sent to a destination with no subscriber.
    @Override
    public void afterMessageHandled(Message<?> message, MessageChannel channel, MessageHandler handler, Exception ex) {
        if (ex != null || !(handler instanceof AbstractBrokerMessageHandler)) {
            return;
        }

        StompHeaderAccessor accessor = MessageHeaderAccessor.getAccessor(message, StompHeaderAccessor.class);
        if (accessor == null || !StompCommand.SUBSCRIBE.equals(accessor.getCommand())) {
            return;
        }

        String userId = resolveUserId(accessor);
        String destination = accessor.getDestination();
        if (userId == null || destination == null) {
            return;
        }

        if (LOBBY_DESTINATION.equals(destination)) {
            broadcastOnlineCount();
            sendLobbySnapshotToUser(userId);
        } else if (destination.startsWith(TABLE_DESTINATION_PREFIX)) {
            tableManager.cancelDisconnectTask(userId);
            sendTableSnapshotToUser(userId, destination.substring(TABLE_DESTINATION_PREFIX.length()));
        }
    }

    public int getOnlineCount() {
        return sessionsByUser.size();
    }

    public void broadcastOnlineCount() {
        messagingTemplate.convertAndSend(LOBBY_DESTINATION, new OnlineUpdateDTO("ONLINE_UPDATE", getOnlineCount()));
    }

    private boolean registerSession(String userId, String sessionId) {
        boolean[] firstSession = new boolean[1];
        sessionsByUser.compute(userId, (key, sessions) -> {
            if (sessions == null) {
                sessions = ConcurrentHashMap.newKeySet();
                firstSession[0] = true;
            }
            sessions.add(sessionId);
            return sessions;
        });
        return firstSession[0];
    }

    private boolean unregisterSession(String userId, String sessionId) {
        boolean[] lastSession = new boolean[1];
        sessionsByUser.computeIfPresent(userId, (key, sessions) -> {
            sessions.remove(sessionId);
            if (sessions.isEmpty()) {
                lastSession[0] = true;
                return null;
            }
            return sessions;
        });
        return lastSession[0];
    }

    private void sendLobbySnapshotToUser(String userId) {
        List<TableDTO> currentLobby = tableManager.getAllTables().stream()
                .map(TableDTO::createTableDTO)
                .toList();

        messagingTemplate.convertAndSendToUser(
                userId,
                "/queue/lobby_snapshot",
                LobbySnapshotDTO.of(currentLobby)
        );
    }

    private void sendTableSnapshotToUser(String userId, String tableId) {
        Table table = tableManager.getTable(tableId);
        if (table == null) {
            return;
        }

        try {
            TableDetailsDTO snapshot = TableDetailsDTO.createTableDetailsDTO(table, userId, true);
            messagingTemplate.convertAndSendToUser(userId, "/queue/table_snapshot", snapshot);
            log.info("Sent TABLE_UPDATE snapshot for table {} to user {}", tableId, userId);
        } catch (Exception e) {
            log.error("Failed to build or send snapshot for table {} to user {}", tableId, userId, e);
        }
    }

    private String resolveUserId(StompHeaderAccessor accessor) {
        Map<String, Object> sessionAttributes = accessor.getSessionAttributes();
        return sessionAttributes == null
                ? null
                : (String) sessionAttributes.get(StompAuthChannelInterceptor.USER_ID_ATTRIBUTE);
    }
}
