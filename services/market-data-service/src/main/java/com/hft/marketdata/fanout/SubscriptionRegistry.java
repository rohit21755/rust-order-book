package com.hft.marketdata.fanout;

import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-connection subscription registry: sessionId → set of channel keys.
 *
 * Stateless w.r.t. clustering — only tracks in-process connections; no session affinity
 * needed because each node maintains its own registry and its own Kafka consumer fan-out.
 */
@Component
public class SubscriptionRegistry {

    private final ConcurrentHashMap<String, Set<String>> subscriptions = new ConcurrentHashMap<>();

    public void register(String sessionId, String channel) {
        subscriptions.computeIfAbsent(sessionId, k -> ConcurrentHashMap.newKeySet()).add(channel);
    }

    public void remove(String sessionId) {
        subscriptions.remove(sessionId);
    }

    public Set<String> channels(String sessionId) {
        return subscriptions.getOrDefault(sessionId, Set.of());
    }

    public int connectionCount() {
        return subscriptions.size();
    }
}
