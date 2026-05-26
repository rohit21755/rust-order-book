package com.hft.order.fix;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

/**
 * Append-only audit log for FIX messages (every inbound + outbound message).
 * Backed by an {@code O_APPEND} file open so concurrent writes never overwrite each other.
 */
@Slf4j
@Component
public class FixAuditLogger {

    private final Path logPath;

    public FixAuditLogger(@Value("${hft.fix.audit-log:/var/log/fix/audit.log}") String logPath) {
        this.logPath = Paths.get(logPath);
        try {
            Files.createDirectories(this.logPath.getParent());
            if (!Files.exists(this.logPath)) Files.createFile(this.logPath);
        } catch (IOException e) {
            log.warn("FIX audit log init failed for {}: {}", this.logPath, e.getMessage());
        }
    }

    public synchronized void record(String direction, String sessionId, String rawMessage) {
        String line = String.format("%s\t%s\t%s\t%s%n",
                Instant.now(), direction, sessionId, rawMessage.replace('', '|'));
        try {
            Files.write(logPath, line.getBytes(StandardCharsets.UTF_8),
                    (OpenOption) StandardOpenOption.CREATE,
                    (OpenOption) StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.error("FIX audit write failed: {}", e.getMessage());
        }
    }
}
