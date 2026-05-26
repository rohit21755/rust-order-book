package com.hft.grpc.config;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Round-robin pool of gRPC channels. gRPC already multiplexes calls over one HTTP/2 connection,
 * but multiple channels spread load across event loops and broaden the connection pool —
 * required by the spec ("gRPC connection pooling in Java client").
 */
@Slf4j
public class GrpcChannelPool implements AutoCloseable {

    private final List<ManagedChannel> channels = new ArrayList<>();
    private final AtomicInteger cursor = new AtomicInteger();

    public GrpcChannelPool(GrpcClientProperties props) {
        int size = Math.max(1, props.getPoolSize());
        for (int i = 0; i < size; i++) {
            channels.add(build(props));
        }
        log.info("gRPC channel pool created size={} target={}:{}", size, props.getHost(), props.getPort());
    }

    private ManagedChannel build(GrpcClientProperties props) {
        NettyChannelBuilder builder = NettyChannelBuilder
                .forAddress(props.getHost(), props.getPort())
                .keepAliveTime(props.getKeepAliveMs(), TimeUnit.MILLISECONDS)
                .keepAliveTimeout(props.getKeepAliveTimeoutMs(), TimeUnit.MILLISECONDS)
                .maxInboundMessageSize(props.getMaxInboundMessageSize());

        if (props.isTlsEnabled()) {
            try {
                SslContext.Builder ssl = GrpcSslContexts.forClient();
                if (props.getTrustCertPath() != null && !props.getTrustCertPath().isBlank()) {
                    ssl.trustManager(new File(props.getTrustCertPath()));
                }
                builder.sslContext(ssl.build());
                if (props.getAuthorityOverride() != null && !props.getAuthorityOverride().isBlank()) {
                    builder.overrideAuthority(props.getAuthorityOverride());
                }
            } catch (Exception e) {
                throw new IllegalStateException("Failed to configure TLS for gRPC channel", e);
            }
        } else {
            builder.usePlaintext();
        }
        return builder.build();
    }

    /** Next channel (round-robin). */
    public ManagedChannel next() {
        int idx = Math.floorMod(cursor.getAndIncrement(), channels.size());
        return channels.get(idx);
    }

    @Override
    @PreDestroy
    public void close() {
        for (ManagedChannel ch : channels) {
            ch.shutdown();
            try {
                if (!ch.awaitTermination(5, TimeUnit.SECONDS)) {
                    ch.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                ch.shutdownNow();
            }
        }
        log.info("gRPC channel pool closed");
    }
}
