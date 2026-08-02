package com.poker.config;

import com.poker.service.AccountService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageDeliveryException;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Authenticates the STOMP session once, when the CONNECT frame arrives. Every later frame is
 * authorized against the principal bound to the session, so a short-lived access token cannot
 * tear down a long-running game connection halfway through a hand.
 */
@Slf4j
@Component
public class StompAuthChannelInterceptor implements ChannelInterceptor {

    public static final String USER_ID_ATTRIBUTE = "userId";
    public static final String JWT_TOKEN_ATTRIBUTE = "jwtToken";

    private static final String BEARER_PREFIX = "Bearer ";

    private final AccountService accountService;

    public StompAuthChannelInterceptor(@Lazy AccountService accountService) {
        this.accountService = accountService;
    }

    @Override
    public Message<?> preSend(Message<?> message, MessageChannel channel) {
        StompHeaderAccessor accessor = MessageHeaderAccessor.getAccessor(message, StompHeaderAccessor.class);

        if (accessor == null || accessor.getCommand() == null) {
            return message;
        }

        StompCommand command = accessor.getCommand();

        if (StompCommand.CONNECT.equals(command) || StompCommand.STOMP.equals(command)) {
            authenticate(accessor);
        } else if (requiresAuthentication(command) && resolveUserId(accessor) == null) {
            log.warn("Rejected {} on unauthenticated session {}", command, accessor.getSessionId());
            throw new MessageDeliveryException(message, "Unauthorized");
        }

        return message;
    }

    private void authenticate(StompHeaderAccessor accessor) {
        Map<String, Object> attributes = accessor.getSessionAttributes();
        if (attributes == null) {
            throw new MessageDeliveryException("Unauthorized");
        }

        String token = extractBearerToken(accessor);
        if (token == null) {
            log.warn("WS CONNECT rejected: no bearer token supplied");
            throw new MessageDeliveryException("Unauthorized");
        }

        String userId;
        try {
            userId = accountService.getUserIdByToken(token);
        } catch (Exception e) {
            log.warn("WS CONNECT rejected: token validation failed ({})", e.getClass().getSimpleName());
            throw new MessageDeliveryException("Unauthorized");
        }

        if (userId == null) {
            log.warn("WS CONNECT rejected: token is expired or malformed");
            throw new MessageDeliveryException("Unauthorized");
        }

        attributes.put(USER_ID_ATTRIBUTE, userId);
        attributes.put(JWT_TOKEN_ATTRIBUTE, token);
        accessor.setUser(new StompPrincipal(userId));
    }

    private boolean requiresAuthentication(StompCommand command) {
        return StompCommand.SUBSCRIBE.equals(command) || StompCommand.SEND.equals(command);
    }

    private String resolveUserId(StompHeaderAccessor accessor) {
        Map<String, Object> attributes = accessor.getSessionAttributes();
        return attributes == null ? null : (String) attributes.get(USER_ID_ATTRIBUTE);
    }

    private String extractBearerToken(StompHeaderAccessor accessor) {
        List<String> authHeaders = accessor.getNativeHeader("Authorization");
        if (authHeaders == null || authHeaders.isEmpty()) {
            return null;
        }

        String rawHeader = authHeaders.get(0);
        if (rawHeader == null || !rawHeader.startsWith(BEARER_PREFIX)) {
            return null;
        }

        String token = rawHeader.substring(BEARER_PREFIX.length()).trim();
        return token.isEmpty() ? null : token;
    }
}
