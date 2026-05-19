package com.hft.order.kafka;

import com.hft.order.config.OrderProperties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public SenderOptions<String, String> senderOptions(OrderProperties props) {
        Map<String, Object> cfg = new HashMap<>();
        var k = props.getKafka();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, k.getBootstrapServers());
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.ACKS_CONFIG, k.getAcks());
        cfg.put(ProducerConfig.RETRIES_CONFIG, 0); // Reactor-Kafka layer retries; producer client retries=0 to avoid double retry
        cfg.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        cfg.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        cfg.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, k.getRequestTimeoutMillis());
        cfg.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, k.getDeliveryTimeoutMillis());
        cfg.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        cfg.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        return SenderOptions.create(cfg);
    }

    @Bean(destroyMethod = "close")
    public KafkaSender<String, String> kafkaSender(SenderOptions<String, String> opts) {
        return KafkaSender.create(opts);
    }
}
