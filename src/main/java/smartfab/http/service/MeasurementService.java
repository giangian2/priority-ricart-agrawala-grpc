package smartfab.http.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import smartfab.http.respository.MeasurementRepository;
import smartfab.model.edge.Measurement;

import java.util.List;
import java.util.Collections;

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
        List<Measurement> measurements = measurementRepository.findAll();
        Collections.sort(measurements); 
        return measurements;
    }
}