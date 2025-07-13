package ru.example.weather;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class WeatherConsumer implements Runnable {
    private static final String TOPIC = "weather-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private final KafkaConsumer<String, WeatherData> consumer;

    private final Map<String, List<WeatherData>> weatherDataByCity = new ConcurrentHashMap<>();
    private volatile boolean running = true;

    public WeatherConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "weather-analytics-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, WeatherDataDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Начать читать с самого начала топика

        this.consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList(TOPIC));
        System.out.println("WeatherConsumer запущен. Ожидание данных...");

        try {
            while (running) {
                ConsumerRecords<String, WeatherData> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, WeatherData> record : records) {
                    WeatherData data = record.value();
                    System.out.printf("Получено: город=%s, температура=%.1f, состояние=%s\n",
                            data.getCity(), data.getTemperature(), data.getCondition());

                    weatherDataByCity.computeIfAbsent(data.getCity(), k -> new ArrayList<>()).add(data);
                }
            }
        } finally {
            consumer.close();
            System.out.println("WeatherConsumer остановлен.");
        }
    }

    public void stop() {
        this.running = false;
    }

    public void analyzeAndPrintResults() {
        System.out.println("\n--- Финальная аналитика по погоде за неделю ---");

        findMostRainyCity();

        findHottestDay();

        findCityWithLowestAvgTemp();

        System.out.println("----------------------------------------------\n");
    }

    private void findMostRainyCity() {
        Map.Entry<String, Long> mostRainyCity = weatherDataByCity.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().stream().filter(d -> "дождь".equals(d.getCondition())).count()
                ))
                .entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .orElse(null);

        if (mostRainyCity != null && mostRainyCity.getValue() > 0) {
            System.out.printf("Самое большое количество дождливых дней (%d) было в городе: %s\n",
                    mostRainyCity.getValue(), mostRainyCity.getKey());
        } else {
            System.out.println("Дождливых дней не зафиксировано.");
        }
    }

    private void findHottestDay() {
        Optional<WeatherData> hottest = weatherDataByCity.values().stream()
                .flatMap(List::stream)
                .max(Comparator.comparingDouble(WeatherData::getTemperature));

        hottest.ifPresent(data -> System.out.printf("Самая жаркая погода (%.1f°C) была %s в городе: %s\n",
                data.getTemperature(), data.getEventDate(), data.getCity()));
    }

    private void findCityWithLowestAvgTemp() {
        Map.Entry<String, Double> cityWithLowestAvg = weatherDataByCity.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().stream().mapToDouble(WeatherData::getTemperature).average().orElse(0.0)
                ))
                .entrySet().stream()
                .min(Map.Entry.comparingByValue())
                .orElse(null);

        if (cityWithLowestAvg != null) {
            System.out.printf("Город с самой низкой средней температурой (%.1f°C): %s\n",
                    cityWithLowestAvg.getValue(), cityWithLowestAvg.getKey());
        }
    }
}