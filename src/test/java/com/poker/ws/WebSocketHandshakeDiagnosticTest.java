package com.poker.ws;

import com.poker.config.StompAuthChannelInterceptor;
import com.poker.config.WebSocketConfig;
import com.poker.service.AccountService;
import com.poker.service.TableManager;
import com.poker.service.WebSocketEventListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.lang.NonNull;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WebSocketHandshakeDiagnosticTest {

    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = {
            DataSourceAutoConfiguration.class,
            HibernateJpaAutoConfiguration.class,
            RedisAutoConfiguration.class,
            FlywayAutoConfiguration.class,
            SecurityAutoConfiguration.class
    })
    @Import({WebSocketConfig.class, StompAuthChannelInterceptor.class, WebSocketEventListener.class})
    static class TestApp {
    }

    @MockitoBean
    AccountService accountService;

    @MockitoBean
    TableManager tableManager;

    @LocalServerPort
    int port;

    private WebSocketStompClient stompClient;

    @BeforeEach
    void setUp() {
        when(tableManager.getAllTables()).thenReturn(List.of());
        stompClient = new WebSocketStompClient(new StandardWebSocketClient());
        stompClient.setMessageConverter(new MappingJackson2MessageConverter());
    }

    private StompSession connect(String authorizationHeader) throws Exception {
        StompHeaders connectHeaders = new StompHeaders();
        if (authorizationHeader != null) {
            connectHeaders.add("Authorization", authorizationHeader);
        }
        return stompClient.connectAsync(
                "ws://localhost:" + port + "/ws-poker",
                new WebSocketHttpHeaders(),
                connectHeaders,
                new StompSessionHandlerAdapter() {
                }).get(5, TimeUnit.SECONDS);
    }

    @Test
    void connectWithoutAuthorizationHeaderIsRejected() {
        assertThatThrownBy(() -> connect(null)).isInstanceOf(ExecutionException.class);
    }

    @Test
    void connectWithAnInvalidTokenIsRejected() {
        when(accountService.getUserIdByToken(anyString())).thenReturn(null);

        assertThatThrownBy(() -> connect("Bearer a.b.c")).isInstanceOf(ExecutionException.class);
    }

    @Test
    void connectWithAnExpiredTokenIsRejected() {
        when(accountService.getUserIdByToken(anyString()))
                .thenThrow(new IllegalStateException("expired"));

        assertThatThrownBy(() -> connect("Bearer a.b.c")).isInstanceOf(ExecutionException.class);
    }

    @Test
    void authenticatedSessionReceivesTheLobbySnapshotOnSubscribe() throws Exception {
        when(accountService.getUserIdByToken(anyString())).thenReturn("42");

        StompSession session = connect("Bearer a.b.c");
        assertThat(session.isConnected()).isTrue();

        CompletableFuture<Object> snapshot = subscribeForPayload(session, "/user/queue/lobby_snapshot");
        session.subscribe("/topic/lobby", new StompSessionHandlerAdapter() {
        });

        Object received = snapshot.completeOnTimeout(null, 5, TimeUnit.SECONDS).get();
        assertThat(received).isInstanceOf(Map.class);

        Map<String, Object> snapshotBody = asMap(received);
        assertThat(snapshotBody).containsEntry("event_type", "LOBBY_UPDATE");
        assertThat(snapshotBody).containsKey("tables");
    }

    @Test
    void authenticatedSessionStaysOpenAcrossSubscriptions() throws Exception {
        when(accountService.getUserIdByToken(anyString())).thenReturn("42");

        StompSession session = connect("Bearer a.b.c");

        session.subscribe("/topic/lobby", new StompSessionHandlerAdapter() {
        });
        session.subscribe("/user/queue/wallet", new StompSessionHandlerAdapter() {
        });

        Thread.sleep(500);
        assertThat(session.isConnected()).isTrue();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object payload) {
        return (Map<String, Object>) payload;
    }

    private CompletableFuture<Object> subscribeForPayload(StompSession session, String destination) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        session.subscribe(destination, new StompSessionHandlerAdapter() {
            @Override
            @NonNull
            public Type getPayloadType(@NonNull StompHeaders headers) {
                return Map.class;
            }

            @Override
            public void handleFrame(@NonNull StompHeaders headers, Object payload) {
                future.complete(payload);
            }
        });
        return future;
    }
}
