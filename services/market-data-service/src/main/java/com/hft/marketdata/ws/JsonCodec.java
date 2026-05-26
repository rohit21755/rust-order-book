package com.hft.marketdata.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.stereotype.Component;

@Component
public class JsonCodec {
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    public String toJson(Object o) {
        try {
            return mapper.writeValueAsString(o);
        } catch (Exception e) {
            return "{\"error\":\"serialization_failed\"}";
        }
    }

    public ObjectMapper mapper() {
        return mapper;
    }
}
