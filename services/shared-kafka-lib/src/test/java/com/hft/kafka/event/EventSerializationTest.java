package com.hft.kafka.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.kafka.config.KafkaJacksonConfig;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class EventSerializationTest {

    private final ObjectMapper mapper = new KafkaJacksonConfig().kafkaObjectMapper();

    @Test
    void orderEventRoundTrip() throws Exception {
        OrderEvent e = new OrderEvent(OrderEvent.Type.NEW, UUID.randomUUID(), UUID.randomUUID(),
                "BTC-USDT", OrderEvent.Side.BUY, OrderEvent.OrderType.LIMIT,
                new BigDecimal("100.5"), null, new BigDecimal("1.0"),
                "idem-1", Instant.parse("2025-01-01T00:00:00Z"));
        byte[] bytes = mapper.writeValueAsBytes(e);
        OrderEvent back = mapper.readValue(bytes, OrderEvent.class);
        assertThat(back).isEqualTo(e);
        assertThat(back.partitionKey()).isEqualTo(e.orderId().toString());
    }

    @Test
    void orderbookUpdateRoundTrip() throws Exception {
        OrderbookUpdateEvent e = new OrderbookUpdateEvent(
                "BTC-USDT", 42L, OrderbookUpdateEvent.Type.DELTA,
                List.of(new OrderbookUpdateEvent.Level(new BigDecimal("100"), new BigDecimal("1"))),
                List.of(new OrderbookUpdateEvent.Level(new BigDecimal("101"), new BigDecimal("2"))),
                Instant.parse("2025-01-01T00:00:00Z"));
        OrderbookUpdateEvent back = mapper.readValue(mapper.writeValueAsBytes(e), OrderbookUpdateEvent.class);
        assertThat(back).isEqualTo(e);
    }

    @Test
    void dlqMessageRoundTrip() throws Exception {
        DLQMessage dlq = new DLQMessage("orders", "k1", "{\"x\":1}", "boom",
                "java.lang.RuntimeException", 3, Instant.now(), java.util.Map.of("x-trace-id", "t"));
        DLQMessage back = mapper.readValue(mapper.writeValueAsBytes(dlq), DLQMessage.class);
        assertThat(back).isEqualTo(dlq);
    }
}
