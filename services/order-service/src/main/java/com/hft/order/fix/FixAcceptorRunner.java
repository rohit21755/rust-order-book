package com.hft.order.fix;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import quickfix.DefaultMessageFactory;
import quickfix.FileLogFactory;
import quickfix.FileStoreFactory;
import quickfix.Initiator;
import quickfix.MessageFactory;
import quickfix.SessionSettings;
import quickfix.SocketAcceptor;

import java.io.InputStream;

/**
 * Spring lifecycle wrapper around QuickFIX/J's {@link SocketAcceptor}.
 * Enabled by {@code hft.fix.enabled=true}; binds {@code hft.fix.port} (default 9876).
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "hft.fix", name = "enabled", havingValue = "true")
public class FixAcceptorRunner {

    private final FixApplication application;

    @Value("${hft.fix.config-classpath:fix/acceptor.cfg}")
    private String configResource;

    private SocketAcceptor acceptor;

    @PostConstruct
    public void start() throws Exception {
        try (InputStream cfg = new ClassPathResource(configResource).getInputStream()) {
            SessionSettings settings = new SessionSettings(cfg);
            FileStoreFactory storeFactory = new FileStoreFactory(settings);
            FileLogFactory logFactory = new FileLogFactory(settings);
            MessageFactory messageFactory = new DefaultMessageFactory();
            acceptor = new SocketAcceptor(application, storeFactory, settings, logFactory, messageFactory);
            acceptor.start();
            log.info("FIX acceptor started on configured port (default 9876)");
        }
    }

    @PreDestroy
    public void stop() {
        if (acceptor != null) {
            acceptor.stop();
            log.info("FIX acceptor stopped");
        }
    }

    // Initiator unused; kept import only for symmetry / future client mode.
    @SuppressWarnings("unused")
    void _initiator(Initiator i) {}
}
