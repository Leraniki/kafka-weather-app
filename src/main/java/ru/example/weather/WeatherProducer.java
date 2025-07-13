package ru.example.weather;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDate;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class WeatherProducer implements Runnable {

    private static final String TOPIC = "weather-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private final KafkaProducer<String, WeatherData> producer;
    private final Random random = new Random();
    private final List<String> cities = List.of("Москва", "Санкт-Петербург", "Тюмень", "Магадан", "Владивосток");
    private final List<String> conditions = List.of("солнечно", "облачно", "дождь");

    private volatile boolean running = true;

    public WeatherProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, WeatherDataSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void run() {
        System.out.println("WeatherProducer запущен. Отправка данных...");
        try {
            while (running) {
                String city = cities.get(random.nextInt(cities.size()));
                WeatherData data = generateRandomWeatherData(city);

                ProducerRecord<String, WeatherData> record = new ProducerRecord<>(TOPIC, city, data);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.printf("Отправлено: город=%s, температура=%.1f\n",
                                data.getCity(), data.getTemperature());
                    } else {
                        System.err.println("Ошибка отправки: " + exception.getMessage());
                    }
                });

                TimeUnit.MILLISECONDS.sleep(500);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("WeatherProducer был прерван.");
        } finally {
            producer.flush();
            producer.close();
            System.out.println("WeatherProducer завершил работу.");
        }
    }

    public void stop() {
        this.running = false;
    }

    private WeatherData generateRandomWeatherData(String city) {
        LocalDate date = LocalDate.now();
        double temperature = random.nextInt(36);
        String condition = conditions.get(random.nextInt(conditions.size()));
        return new WeatherData(city, temperature, condition, date);
    }
}