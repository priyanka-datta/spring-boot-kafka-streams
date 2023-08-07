package com.priyanka.datta.kafkastreamsprogram.dto;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

public class CustomDeSerializer<T> implements Deserializer<T> {

    ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public void configure(Map configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        com.priyanka.datta.kafkastreamsprogram.dto.CustomDto customDto = new com.priyanka.datta.kafkastreamsprogram.dto.CustomDto();
        try {
            customDto = objectMapper.readValue(bytes, com.priyanka.datta.kafkastreamsprogram.dto.CustomDto.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return (T) customDto;
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
