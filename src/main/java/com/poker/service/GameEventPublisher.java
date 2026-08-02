package com.poker.service;

import com.poker.dto.TableDTO;
import com.poker.dto.events.LobbySnapshotDTO;
import com.poker.dto.events.TableDetailsDTO;
import com.poker.dto.events.*;
import com.poker.util.RedisTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class GameEventPublisher {

    private final RedisTemplate<String, Object> redisTemplate;

    public void publishTableUpdate(TableDetailsDTO tableDetails) {
        publish(RedisTopics.getTableTopic(tableDetails.tableId()), tableDetails);
    }

    public void publishPlayerAction(PlayerActionEvent event) {
        publish(RedisTopics.getTableTopic(event.tableId()), event);
    }

    public void publishPlayerStatus(PlayerStatusEvent event) {
        publish(RedisTopics.getTableTopic(event.tableId()), event);
    }

    public void publishFullLobbyUpdate(List<TableDTO> tables) {
        publish("poker:lobby", LobbySnapshotDTO.of(tables));
    }

    public void publishLobbyUpdate(String tableId, int currentPlayers, int maxPlayers) {
        publish("poker:lobby", new LobbyTableUpdateDTO("LOBBY_UPDATE", tableId, currentPlayers, maxPlayers));
    }

    public void publishWalletUpdate(String userId, long newBalance, String reason) {
        publish("poker:wallet:" + userId, new WalletUpdateEvent(userId, newBalance, reason));
    }

    public void publishStreetEnd(StreetEndDTO event) {
        publish(RedisTopics.getTableTopic(event.tableId()), event);
    }

    /**
     * Publishing must never propagate: these calls happen inside the table lock and on the turn
     * timer thread, so a Redis outage would otherwise abort a hand or kill the one-shot timer and
     * leave the table frozen for every client.
     */
    private void publish(String topic, Object payload) {
        try {
            redisTemplate.convertAndSend(topic, payload);
        } catch (Exception e) {
            log.error("Failed to publish event to Redis topic {}", topic, e);
        }
    }
}
