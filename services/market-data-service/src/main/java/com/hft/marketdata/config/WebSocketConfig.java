package com.hft.marketdata.config;

import com.hft.marketdata.ws.OrderbookWebSocketHandler;
import com.hft.marketdata.ws.PortfolioWebSocketHandler;
import com.hft.marketdata.ws.TickerWebSocketHandler;
import com.hft.marketdata.ws.TradesWebSocketHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps WebSocket paths to handlers. Ant-style {@code /*} captures the trailing
 * {symbol} which each handler extracts from the URI.
 */
@Configuration
public class WebSocketConfig {

    @Bean
    public HandlerMapping webSocketHandlerMapping(
            OrderbookWebSocketHandler orderbook,
            TradesWebSocketHandler trades,
            TickerWebSocketHandler ticker,
            PortfolioWebSocketHandler portfolio) {

        Map<String, WebSocketHandler> map = new LinkedHashMap<>();
        map.put("/ws/orderbook/*", orderbook);
        map.put("/ws/trades/*", trades);
        map.put("/ws/ticker/*", ticker);
        map.put("/ws/portfolio", portfolio);

        SimpleUrlHandlerMapping mapping = new SimpleUrlHandlerMapping();
        mapping.setUrlMap(map);
        mapping.setOrder(-1); // before annotated controllers
        return mapping;
    }

    @Bean
    public WebSocketHandlerAdapter webSocketHandlerAdapter() {
        return new WebSocketHandlerAdapter();
    }
}
