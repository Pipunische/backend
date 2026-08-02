package com.poker.dto.events;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.poker.dto.TableDTO;

import java.util.List;

public record LobbySnapshotDTO(
        @JsonProperty("event_type") String eventType,
        @JsonProperty("tables") List<TableDTO> tables
) {
    public static LobbySnapshotDTO of(List<TableDTO> tables) {
        return new LobbySnapshotDTO("LOBBY_UPDATE", tables);
    }
}
