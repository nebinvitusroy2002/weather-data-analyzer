package com.aggregator.weatherDataAggregator.repository;

import com.aggregator.weatherDataAggregator.model.WeatherData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;

@Repository
public interface WeatherDataRepository extends JpaRepository<WeatherData,Long> {
    List<WeatherData> findByDateBetween(LocalDate startDate,LocalDate endDate);
}
