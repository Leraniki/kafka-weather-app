package ru.example.weather;

import java.util.concurrent.TimeUnit;

public class WeatherApp {
    public static void main(String[] args) {
        System.out.println("Запуск приложения...");

        WeatherConsumer consumer = new WeatherConsumer();
        WeatherProducer producer = new WeatherProducer();

        Thread consumerThread = new Thread(consumer);
        Thread producerThread = new Thread(producer);

        consumerThread.start();
        producerThread.start();

        try {
            System.out.println("Сбор данных будет идти в течение 20 секунд...");
            TimeUnit.SECONDS.sleep(20);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("\nВремя вышло. Остановка сбора данных...");

        producer.stop();
        consumer.stop();

        try {
            System.out.println("Ожидание завершения работы потоков...");
            producerThread.join(2000);
            consumerThread.join(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Все потоки остановлены.");

        consumer.analyzeAndPrintResults();

        System.out.println("Приложение завершило работу.");
    }
}