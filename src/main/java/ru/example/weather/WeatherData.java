package ru.example.weather;

import java.time.LocalDate;
public class WeatherData {
    private String city;
    private double temperature;
    private String condition;
    private LocalDate eventDate;


    public WeatherData() {}

    public WeatherData(String city, double temperature, String condition, LocalDate eventDate) {
        this.city = city;
        this.temperature = temperature;
        this.condition = condition;
        this.eventDate = eventDate;
    }


    public String getCity() { return city; }
    public double getTemperature() { return temperature; }
    public String getCondition() { return condition; }
    public LocalDate getEventDate() { return eventDate; }

    @Override
    public String toString() {
        return "WeatherData{" +
                "city='" + city + '\'' +
                ", temperature=" + temperature +
                ", condition='" + condition + '\'' +
                ", eventDate=" + eventDate +
                '}';
    }
}