package com.hft.kafka.event;

/** Marker for all platform event records. Every event must expose a stable partition key. */
public interface EventEnvelope {
    /** Used as Kafka record key for partition ordering. */
    String partitionKey();
}
