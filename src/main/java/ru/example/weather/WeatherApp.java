package ru.example.weather;

public class WeatherApp {
    public static void main(String[] args) throws InterruptedException {
        WeatherConsumer consumer = new WeatherConsumer();
        WeatherProducer producer = new WeatherProducer();

        Thread consumerThread = new Thread(consumer);
        Thread producerThread = new Thread(producer);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Приложение останавливается. Запуск финального анализа...");
            consumer.stop();
            try {

                consumerThread.join(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            consumer.analyzeAndPrintResults();
        }));

        consumerThread.start();

        Thread.sleep(2000);

        producerThread.start();
    }
}