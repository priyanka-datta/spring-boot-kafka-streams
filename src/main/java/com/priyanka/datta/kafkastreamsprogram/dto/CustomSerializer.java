package com.priyanka.datta.kafkastreamsprogram.dto;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class CustomSerializer<CustomDto> implements Serializer<CustomDto> {

    ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, CustomDto customDto) {
        try {
            return objectMapper.writeValueAsBytes(customDto);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] serialize(String topic, Headers headers, CustomDto data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
