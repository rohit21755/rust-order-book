package com.hft.kafka.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.HashMap;
import java.util.Map;

/** Builds reactor-kafka {@link KafkaSender} instances from {@link SharedKafkaProperties}. */
public class KafkaSenderFactory {

    private final SharedKafkaProperties props;

    public KafkaSenderFactory(SharedKafkaProperties props) {
        this.props = props;
    }

    public KafkaSender<String, byte[]> create() {
        return KafkaSender.create(senderOptions());
    }

    public SenderOptions<String, byte[]> senderOptions() {
        Map<String, Object> cfg = new HashMap<>();
        SharedKafkaProperties.Producer p = props.getProducer();

        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getBootstrapServers());
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        cfg.put(ProducerConfig.ACKS_CONFIG, p.getAcks());
        cfg.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, p.isIdempotence());
        cfg.put(ProducerConfig.RETRIES_CONFIG, p.getRetries());
        cfg.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, p.getMaxInFlightPerConnection());
        cfg.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, p.getRequestTimeoutMs());
        cfg.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, p.getDeliveryTimeoutMs());
        cfg.put(ProducerConfig.LINGER_MS_CONFIG, p.getLingerMs());
        cfg.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, p.getCompressionType());

        if (p.getTransactionalId() != null && !p.getTransactionalId().isBlank()) {
            cfg.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, p.getTransactionalId());
        }
        cfg.putAll(p.getExtraProps());

        return SenderOptions.create(cfg);
    }
}
