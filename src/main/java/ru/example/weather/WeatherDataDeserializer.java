package ru.example.weather;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;

public class WeatherDataDeserializer implements Deserializer<WeatherData> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public WeatherDataDeserializer() {
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public WeatherData deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, WeatherData.class);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing JSON into WeatherData", e);
        }
    }
}