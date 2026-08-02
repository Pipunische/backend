package com.poker.config;

import com.poker.service.WebSocketEventListener;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketTransportRegistration;

@Slf4j
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    private static final long HEARTBEAT_INTERVAL_MS = 20_000;
    private static final int MESSAGE_SIZE_LIMIT = 128 * 1024;
    private static final int SEND_BUFFER_SIZE_LIMIT = 1024 * 1024;
    private static final int SEND_TIME_LIMIT_MS = 20_000;

    private final StompAuthChannelInterceptor authChannelInterceptor;
    private final WebSocketEventListener webSocketEventListener;

    private ThreadPoolTaskScheduler heartbeatScheduler;

    public WebSocketConfig(StompAuthChannelInterceptor authChannelInterceptor,
                           @Lazy WebSocketEventListener webSocketEventListener) {
        this.authChannelInterceptor = authChannelInterceptor;
        this.webSocketEventListener = webSocketEventListener;
    }

    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
        this.heartbeatScheduler = new ThreadPoolTaskScheduler();
        this.heartbeatScheduler.setPoolSize(2);
        this.heartbeatScheduler.setThreadNamePrefix("ws-heartbeat-");
        this.heartbeatScheduler.setDaemon(true);
        this.heartbeatScheduler.initialize();

        config.enableSimpleBroker("/topic", "/queue")
                .setHeartbeatValue(new long[]{HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS})
                .setTaskScheduler(this.heartbeatScheduler);

        config.setApplicationDestinationPrefixes("/app");
        config.setUserDestinationPrefix("/user");

        // Game state is only meaningful in order: without this a client can render a stale
        // TABLE_UPDATE after a newer one because frames for one session may run on different threads.
        config.setPreservePublishOrder(true);
    }

    @PreDestroy
    public void shutdownHeartbeatScheduler() {
        if (this.heartbeatScheduler != null) {
            this.heartbeatScheduler.shutdown();
        }
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws-poker")
                .setAllowedOriginPatterns("*");
    }

    @Override
    public void configureWebSocketTransport(WebSocketTransportRegistration registration) {
        registration.setMessageSizeLimit(MESSAGE_SIZE_LIMIT);
        registration.setSendBufferSizeLimit(SEND_BUFFER_SIZE_LIMIT);
        registration.setSendTimeLimit(SEND_TIME_LIMIT_MS);
    }

    @Override
    public void configureClientInboundChannel(ChannelRegistration registration) {
        registration.interceptors(authChannelInterceptor, webSocketEventListener);
    }
}
