package com.aggregator.weatherDataAggregator.service;

import com.aggregator.weatherDataAggregator.exception.FileUploadException;
import com.aggregator.weatherDataAggregator.model.WeatherData;
import com.aggregator.weatherDataAggregator.repository.WeatherDataRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
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

            Iterable<CSVRecord> records;
            try {
                log.info("Parsing csv file...");
                records = format.parse(reader);
            }catch (IOException e) {
                log.error("Error parsing the CSV file.", e);
                throw new FileUploadException("Error parsing the CSV file.", e);
            }

            for (CSVRecord record : records) {
                WeatherData data = new WeatherData();
                try {
                    data.setDate(LocalDate.parse(record.get("Date")));
                    data.setTemperature(Double.parseDouble(record.get("Temperature (°C)")));
                    data.setHumidity(Double.parseDouble(record.get("Humidity (%)")));
                    data.setRainfall(Double.parseDouble(record.get("Rainfall (mm)")));
                    data.setLocation(record.get("Location"));
                    repository.save(data);
                    log.debug("Saved weather data for location: {} on date: {}", data.getLocation(), data.getDate());
                }catch (NumberFormatException | DateTimeParseException e) {
                    log.error("Error parsing record: {}", record, e);
                    throw new FileUploadException("Error parsing a record in the CSV file.", e);
                }catch (Exception e) {
                    log.error("Failed to save data for record: {}", record, e);
                    throw new FileUploadException("Failed to save data to the repository.", e);
                }
                log.info("CSV file uploaded and data saved successfully.");
            }
        }catch (FileUploadException e) {
            log.error("File upload failed due to an error.", e);
            throw e; // Re-throwing to propagate the error
        } catch (Exception e) {
            log.error("Unexpected error during file upload.", e);
            throw new FileUploadException("An unexpected error occurred during file upload.", e);
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
                double avgTemp = locationData.stream().mapToDouble(WeatherData::getTemperature).average().orElse(0);
                double avgHumidity = locationData.stream().mapToDouble(WeatherData::getHumidity).average().orElse(0);
                double avgRainfall = locationData.stream().mapToDouble(WeatherData::getRainfall).average().orElse(0);
                report.append("Location: ").append(location).append("\n")
                        .append("Avg Temp: ").append(avgTemp).append(", Avg Humidity: ").append(avgHumidity)
                        .append(", Avg Rainfall: ").append(avgRainfall).append("\n").append("\n");
            }
            log.info("Weather data fetched and report generated.");
            return report.toString();
        } catch (Exception e) {
            log.error("Error fetching weather data for the date range.", e);
            throw new FileUploadException("Error fetching weather data.", e);
        }
    }
}

