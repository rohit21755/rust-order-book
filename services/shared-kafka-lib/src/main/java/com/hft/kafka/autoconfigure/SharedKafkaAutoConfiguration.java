package com.hft.kafka.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.kafka.admin.KafkaTopicAdmin;
import com.hft.kafka.config.KafkaJacksonConfig;
import com.hft.kafka.config.KafkaReceiverFactory;
import com.hft.kafka.config.KafkaSenderFactory;
import com.hft.kafka.config.SharedKafkaProperties;
import com.hft.kafka.dlq.DLQAdminController;
import com.hft.kafka.dlq.DLQMonitor;
import com.hft.kafka.dlq.DLQReplayService;
import com.hft.kafka.dlq.DLQRouter;
import com.hft.kafka.producer.KafkaEventPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import reactor.kafka.sender.KafkaSender;

@AutoConfiguration
@ConditionalOnClass(KafkaSender.class)
@EnableConfigurationProperties(SharedKafkaProperties.class)
@Import(KafkaJacksonConfig.class)
public class SharedKafkaAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public KafkaSenderFactory kafkaSenderFactory(SharedKafkaProperties props) {
        return new KafkaSenderFactory(props);
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public KafkaSender<String, byte[]> kafkaSender(KafkaSenderFactory factory) {
        return factory.create();
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaReceiverFactory kafkaReceiverFactory(SharedKafkaProperties props) {
        return new KafkaReceiverFactory(props);
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaEventPublisher<com.hft.kafka.event.EventEnvelope> kafkaEventPublisher(
            KafkaSender<String, byte[]> sender,
            @Qualifier("kafkaObjectMapper") ObjectMapper mapper,
            SharedKafkaProperties props
    ) {
        return new KafkaEventPublisher<>(sender, mapper, props);
    }

    @Bean
    @ConditionalOnMissingBean
    public DLQRouter dlqRouter(KafkaSender<String, byte[]> sender,
                               @Qualifier("kafkaObjectMapper") ObjectMapper mapper,
                               SharedKafkaProperties props) {
        return new DLQRouter(sender, mapper, props);
    }

    @Bean
    @ConditionalOnMissingBean
    public DLQReplayService dlqReplayService(KafkaReceiverFactory receiverFactory,
                                             KafkaSender<String, byte[]> sender,
                                             @Qualifier("kafkaObjectMapper") ObjectMapper mapper,
                                             SharedKafkaProperties props) {
        return new DLQReplayService(receiverFactory, sender, mapper, props);
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public KafkaTopicAdmin kafkaTopicAdmin(SharedKafkaProperties props) {
        return new KafkaTopicAdmin(props);
    }

    @Bean
    @ConditionalOnProperty(prefix = "hft.kafka.dlq", name = "monitor-enabled", matchIfMissing = false)
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnMissingBean
    public DLQMonitor dlqMonitor(KafkaReceiverFactory receiverFactory,
                                 @Qualifier("kafkaObjectMapper") ObjectMapper mapper,
                                 SharedKafkaProperties props,
                                 MeterRegistry meterRegistry) {
        return new DLQMonitor(receiverFactory, mapper, props, meterRegistry);
    }

    @Bean
    @ConditionalOnClass(name = "org.springframework.web.bind.annotation.RestController")
    @ConditionalOnProperty(prefix = "hft.kafka.dlq", name = "admin-endpoint-enabled", matchIfMissing = false)
    @ConditionalOnMissingBean
    public DLQAdminController dlqAdminController(DLQReplayService replayService) {
        return new DLQAdminController(replayService);
    }
}
