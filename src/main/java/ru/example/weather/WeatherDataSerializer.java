package ru.example.weather;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serializer;

public class WeatherDataSerializer implements Serializer<WeatherData> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public WeatherDataSerializer() {
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public byte[] serialize(String topic, WeatherData data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing WeatherData to JSON", e);
        }
    }
}