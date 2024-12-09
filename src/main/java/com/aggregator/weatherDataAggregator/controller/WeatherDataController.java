package com.aggregator.weatherDataAggregator.controller;

import com.aggregator.weatherDataAggregator.service.WeatherDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.time.DateTimeException;
import java.time.LocalDate;

@RestController
@RequestMapping("/weather")
public class WeatherDataController {

    @Autowired
    private WeatherDataService service;

    @PostMapping("/uploadFile")
    public ResponseEntity<String> uploadCSV(@RequestParam("file")MultipartFile file) {
        if (file.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("File is empty!");
        }
        service.uploadCSV(file);
        return ResponseEntity.status(HttpStatus.OK).body("File uploaded successfully!");
    }

    @GetMapping("/average")
    public ResponseEntity<?> getAverageByDateRange(
            @RequestParam("startDate") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE)LocalDate startDate,
            @RequestParam("endDate") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate endDate){
        String averageData = service.getAverageByDateRange(startDate,endDate);
        if (averageData == null){
            return ResponseEntity.status(HttpStatus.NO_CONTENT).body("No data found!");
        }
        return ResponseEntity.ok(averageData);
    }

}
