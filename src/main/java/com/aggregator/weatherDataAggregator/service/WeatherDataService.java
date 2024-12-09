package com.aggregator.weatherDataAggregator.service;

import com.aggregator.weatherDataAggregator.exception.FileUploadException;
import com.aggregator.weatherDataAggregator.model.WeatherData;
import com.aggregator.weatherDataAggregator.repository.WeatherDataRepository;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStreamReader;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class WeatherDataService {

    //private static final Logger log = LoggerFactory.getLogger(WeatherDataService.class);

    @Autowired
    private WeatherDataRepository repository;

    public void uploadCSV(MultipartFile file) {
        log.info("Starting csv file upload...");
        try {
            if (file.isEmpty()) {
                log.error("The uploaded file is empty.");
                throw new FileUploadException("File is empty");
            }
            InputStreamReader reader = new InputStreamReader(file.getInputStream());
            CSVFormat format = CSVFormat.DEFAULT.builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .build();
            Iterable<CSVRecord> records = format.parse(reader);
            log.info("Parsing csv file...");
            for (CSVRecord record : records) {
                WeatherData data = new WeatherData();
                data.setDate(LocalDate.parse(record.get("Date")));
                data.setTemperature(Double.parseDouble(record.get("Temperature (°C)")));
                data.setHumidity(Double.parseDouble(record.get("Humidity (%)")));
                data.setRainfall(Double.parseDouble(record.get("Rainfall (mm)")));
                data.setLocation(record.get("Location"));
                repository.save(data);
                log.debug("Saved weather data for location: {} on date: {}", data.getLocation(), data.getDate());
            }
            log.info("CSV file uploaded and data saved successfully.");
        } catch (Exception e) {
            log.error("Error processing the csv file.", e);
            throw new FileUploadException("Error processing the CSV file.", e);
        }
    }

    public String getAverageByDateRange(LocalDate startDate, LocalDate endDate) {
        log.info("Fetching weather data for date range: {} to {}", startDate, endDate);
        try {
            List<WeatherData> dataList = repository.findByDateBetween(startDate, endDate);
            if (dataList.isEmpty()) {
                log.warn("No data found for the given date range: {} to {}", startDate, endDate);
                return "No data found";
            }
            Map<String, List<WeatherData>> groupedByLocation = dataList.stream()
                    .collect(Collectors.groupingBy(WeatherData::getLocation));
            StringBuilder report = new StringBuilder();
            for (Map.Entry<String, List<WeatherData>> entry : groupedByLocation.entrySet()) {
                String location = entry.getKey();
                List<WeatherData> locationData = entry.getValue();
                double avgTemp = dataList.stream().mapToDouble(WeatherData::getTemperature).average().orElse(0);
                double avgHumidity = dataList.stream().mapToDouble(WeatherData::getHumidity).average().orElse(0);
                double avgRainfall = dataList.stream().mapToDouble(WeatherData::getRainfall).average().orElse(0);
                report.append("Location: ").append(location).append("\n");
                report.append("Avg Temp: ").append(avgTemp).append(", Avg Humidity: ").append(avgHumidity)
                        .append(", Avg Rainfall: ").append(avgRainfall).append("\n");
            }
            log.info("Weather data fetched and report generated.");
            return report.toString();
        } catch (Exception e) {
            log.error("Error fetching weather data for the date range.", e);
            throw new FileUploadException("Error fetching weather data.", e);
        }
    }
}

