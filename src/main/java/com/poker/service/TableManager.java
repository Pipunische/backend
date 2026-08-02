package com.poker.service;

import com.poker.dto.events.StreetEndDTO;
import com.poker.dto.events.TableDetailsDTO;
import com.poker.dto.events.PlayerStatusEvent;
import com.poker.exception.ChipAmountException;
import com.poker.exception.IllegalTableStateException;
import com.poker.model.*;
import com.poker.persistence.entity.Account;
import com.poker.persistence.entity.GameTable;
import com.poker.persistence.repository.GameTableRepository;
import com.poker.util.TableEventListener;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

@Slf4j
@Service
public class TableManager implements TableEventListener {
    private final Map<String, Table> tables = new ConcurrentHashMap<>();
    private final Map<String, String> activePlayers = new ConcurrentHashMap<>();
    private final AccountService accountService;
    private final GameTableRepository tableRepository;
    private final GameEventPublisher eventPublisher;

    private final org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder passwordEncoder;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Map<String, ScheduledFuture<?>> disconnectTasks = new ConcurrentHashMap<>();

    private static final int DISCONNECT_GRACE_PERIOD = 60;

    public TableManager(AccountService accountService,
                        GameTableRepository tableRepository,
                        GameEventPublisher eventPublisher,
                        org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder passwordEncoder) {
        this.accountService = accountService;
        this.tableRepository = tableRepository;
        this.eventPublisher = eventPublisher;
        this.passwordEncoder = passwordEncoder;
    }

    @Override
    public void onTableUpdate(Table table) {
        TableDetailsDTO dto = TableDetailsDTO.createTableDetailsDTO(table, null);
        eventPublisher.publishTableUpdate(dto);
    }

    @Override
    public void onPlayerAction(String tableId, Player player, ActionType type, long amount, long pot) {
        Table table = tables.get(tableId);

        long timeToActMs = 0;
        int currentTurnSeat = -1;

        if (table != null) {
            currentTurnSeat = table.getActivePlayerIdx();
            if (currentTurnSeat != -1 && table.getState() != TableStates.SHOWDOWN) {
                long elapsed = System.currentTimeMillis() - table.getTurnStartTime();
                timeToActMs = Math.max(0, 15000 - elapsed);
            }
        }

        var actionEvent = new com.poker.dto.events.PlayerActionEvent(
                "PLAYER_ACTION",
                tableId,
                player.getSeatIndex(),
                type,
                amount,
                com.poker.dto.events.PlayerPublicStateDTO.fromPlayer(player),
                pot,
                currentTurnSeat,
                timeToActMs
        );

        if (table != null) {
            table.bufferEvent(actionEvent);
        }

        eventPublisher.publishPlayerAction(actionEvent);
    }

    @Override
    public void onPlayerLeave(String userId, long chips, int seatIndex) {
        String tableId = activePlayers.get(userId);
        if (tableId == null) return;

        accountService.depositToWallet(Long.parseLong(userId), chips, tableId, TransactionType.CASH_OUT);

        String realNickname = "Unknown";
        try {
            Account account = accountService.findById(Long.parseLong(userId));
            realNickname = account.getNickname();
        } catch (Exception ignored) {

        }

        var statusEvent = new PlayerStatusEvent(
                "PLAYER_STATUS",
                tableId,
                seatIndex,
                "LEFT",
                realNickname
        );

        Table table = tables.get(tableId);
        if (table != null) {
            table.bufferEvent(statusEvent);
        }

        eventPublisher.publishPlayerStatus(statusEvent);

        unregisterPlayer(userId);

        if (table != null) {
            eventPublisher.publishLobbyUpdate(tableId, table.getPlayerCount(), table.getMaxPlayers());

            if (table.getPlayerCount() == 0) {
                try {
                    UUID uuid = UUID.fromString(tableId);
                    tableRepository.findById(uuid).ifPresent(dbTable -> {

                        if (!dbTable.getIsSystem()) {
                            tables.remove(tableId);
                            tableRepository.delete(dbTable);

                            eventPublisher.publishLobbyUpdate(tableId, -1, table.getMaxPlayers());

                            log.info("Custom table [{}] was destroyed because it became empty.", dbTable.getName());
                        }
                    });
                } catch (Exception e) {
                    log.error("Error trying to delete empty custom table: {}", tableId, e);
                }
            }

            broadcastLobbyUpdate();
        }
    }

    @Override
    public void onPlayerJoin(String tableId, Player player) {
        var statusEvent = new PlayerStatusEvent(
                "PLAYER_STATUS",
                tableId,
                player.getSeatIndex(),
                "JOINED",
                player.getName()
        );

        Table table = tables.get(tableId);

        if (table != null) {
            table.bufferEvent(statusEvent);
            eventPublisher.publishPlayerStatus(statusEvent);
            eventPublisher.publishLobbyUpdate(tableId, table.getPlayerCount(), table.getMaxPlayers());
        } else {
            eventPublisher.publishPlayerStatus(statusEvent);
        }

        broadcastLobbyUpdate();
    }

    @Override
    public void onStreetEnd(StreetEndDTO event) {
        Table table = tables.get(event.tableId());
        if (table != null) {
            table.bufferEvent(event);
        }
        eventPublisher.publishStreetEnd(event);
    }

    public void forceKickPlayer(String userId) {
        String tableId = activePlayers.get(userId);
        if (tableId != null) {
            Table table = getTable(tableId);
            if (table != null) {
                table.findPlayerById(userId).ifPresent(table::leaveTable);
            }
        }
    }

    public void scheduleDisconnectKick(String userId) {
        if (isPlayerActive(userId)) {
            cancelDisconnectTask(userId);

            ScheduledFuture<?> task = scheduler.schedule(() -> {
                if (isPlayerActive(userId)) {
                    forceKickPlayer(userId);
                    log.info("User {} was auto-kicked after {}s grace period.", userId, DISCONNECT_GRACE_PERIOD);
                }
                disconnectTasks.remove(userId);
            }, DISCONNECT_GRACE_PERIOD, TimeUnit.SECONDS);

            disconnectTasks.put(userId, task);
            log.info("Started {}s disconnect grace period timer for User {}", DISCONNECT_GRACE_PERIOD, userId);
        }
    }

    public void cancelDisconnectTask(String userId) {
        ScheduledFuture<?> task = disconnectTasks.remove(userId);
        if (task != null && !task.isDone()) {
            task.cancel(false);
            log.info("Cancelled disconnect grace period for User {}", userId);
        }
    }

    @Transactional
    public TableDetailsDTO createTable(String name, long smallBlind, long bigBlind, int minPlayersNum,
                                       int maxPlayersNum, String userId, long chips, String rawPasscode) {
        if (activePlayers.containsKey(userId)) {
            throw new IllegalTableStateException("error.player.already.playing");
        }

        String tableIdStr = UUID.randomUUID().toString();
        while (tables.containsKey(tableIdStr)) {
            tableIdStr = UUID.randomUUID().toString();
        }

        UUID tableUuid = UUID.fromString(tableIdStr);
        boolean isPrivate = rawPasscode != null && !rawPasscode.isBlank();

        String hashedPasscode = null;
        if (isPrivate) {
            hashedPasscode = passwordEncoder.encode(rawPasscode);
        }

        Long uId = Long.parseLong(userId);
        Account account = accountService.findById(uId);

        long minBuyIn = chips;

        long maxBuyIn = bigBlind * 100;

        if (minBuyIn > maxBuyIn) {
            throw new ChipAmountException("error.chips.max.buyin", maxBuyIn);
        }

        GameTable dbTable = new GameTable(
                tableUuid,
                name,
                smallBlind,
                bigBlind,
                minPlayersNum,
                maxPlayersNum,
                minBuyIn,
                maxBuyIn,
                isPrivate,
                hashedPasscode,
                false,
                account
        );
        tableRepository.save(dbTable);

        Table newTable = new Table(
                tableIdStr,
                name,
                smallBlind,
                bigBlind,
                minPlayersNum,
                maxPlayersNum,
                minBuyIn,
                isPrivate,
                hashedPasscode,
                this
        );
        tables.put(tableIdStr, newTable);

        try {
            accountService.withdrawFromWallet(uId, chips, tableIdStr, TransactionType.BUY_IN);
            int seatIndex = newTable.getFreeSeat();

            Player creator = new Player(
                    userId,
                    account.getNickname(),
                    account.getAvatarFilename(),
                    seatIndex,
                    new java.util.concurrent.atomic.AtomicLong(account.getBalance()),
                    new java.util.concurrent.atomic.AtomicLong(chips)
            );

            newTable.joinTable(creator);
            registerPlayer(userId, tableIdStr);

        } catch (Exception e) {
            tables.remove(tableIdStr);
            throw e;
        }

        broadcastLobbyUpdate();

        return TableDetailsDTO.createTableDetailsDTO(newTable, userId);
    }

    public Table getTable(String id) { return tables.get(id); }
    public Table removeTable(String id) { return tables.remove(id); }
    public List<Table> getAllTables() { return new ArrayList<>(tables.values()); }

    public void registerPlayer(String userId, String tableId) {
        activePlayers.put(userId, tableId);
        cancelDisconnectTask(userId);
    }
    public void unregisterPlayer(String userId) {
        activePlayers.remove(userId);
        cancelDisconnectTask(userId);
    }

    public boolean isPlayerActive(String userId) { return activePlayers.containsKey(userId); }
    public String getTableIdByPlayer(String userId) { return activePlayers.get(userId); }

    @PostConstruct
    public void initSystemTables() {
        List<GameTable> systemTables = tableRepository.findByIsSystemTrue();
        for (GameTable dbTable : systemTables) {
            Table memoryTable = new Table(
                    dbTable.getId().toString(),
                    dbTable.getName(),
                    dbTable.getSmallBlind(),
                    dbTable.getBigBlind(),
                    dbTable.getMinPlayers(),
                    dbTable.getMaxPlayers(),
                    dbTable.getMinBuyIn(),
                    dbTable.getIsPrivate(),
                    dbTable.getPasscode(),
                    this
            );
            tables.put(memoryTable.getId(), memoryTable);
        }
    }

    @PreDestroy
    public void onServerShutdown() {
        log.warn("CRITICAL: Server is shutting down! Initiating emergency refunds for all active players...");

        for (Table table : tables.values()) {
            for (Player player : table.getPlayers()) {
                try {
                    long currentStack = player.getChips().get();

                    long moneyInPot = player.getTotalInHand();

                    long totalRefund = currentStack + moneyInPot;

                    if (totalRefund > 0) {
                        accountService.depositToWallet(
                                Long.parseLong(player.getUserId()),
                                totalRefund,
                                table.getId(),
                                TransactionType.SYSTEM_REFUND
                        );
                        log.info("Emergency refund: Returned {} chips to user {} (Stack: {}, In Pot: {})",
                                totalRefund, player.getUserId(), currentStack, moneyInPot);
                    }
                } catch (Exception e) {
                    log.error("Failed to refund chips to user {} at table {} during shutdown!",
                            player.getUserId(), table.getId(), e);
                }
            }
        }

        log.info("Emergency refunds completed successfully.");
    }

    @Override
    public void onHandFinished(List<String> playersInHand, Map<String, Long> winnersAndAmounts) {
        for (String userId : playersInHand) {
            boolean isWinner = winnersAndAmounts.containsKey(userId);
            long amountWon = isWinner ? winnersAndAmounts.get(userId) : 0;

            accountService.updatePlayerStats(userId, isWinner, amountWon);
        }
    }

    public void broadcastLobbyUpdate() {
        List<com.poker.dto.TableDTO> currentLobby = getAllTables().stream()
                .map(com.poker.dto.TableDTO::createTableDTO)
                .toList();
        eventPublisher.publishFullLobbyUpdate(currentLobby);
    }
}