package smartfab.http.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import smartfab.http.respository.MeasurementRepository;
import smartfab.model.edge.Measurement;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class MeasurementService {

    @Autowired
    private MeasurementRepository measurementRepository;

    public void addMeasurement(Measurement measurement) {
        measurementRepository.save(measurement.id(), measurement);
    }

    public Measurement getMeasurement(String id) {
        return measurementRepository.findById(id);
    }

    public List<Measurement> getAllMeasurementsSorted() {
        return measurementRepository.findAll()
                .stream()
                .sorted()
                .collect(Collectors.toList());
    }
}