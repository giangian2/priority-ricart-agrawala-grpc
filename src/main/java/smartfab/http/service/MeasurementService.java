package smartfab.http.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import smartfab.http.respository.LinesMeasurementRepository;
import smartfab.model.edge.Measurement;

import java.util.Collections;
import java.util.List;

@Service
public class MeasurementService {

    @Autowired
    private LinesMeasurementRepository measurementRepository;

    public void addMeasurement(String p, Measurement m){
        this.measurementRepository.save(p, Collections.singletonList(m));
    }

    public List<Measurement> getLineMeasurements(String p){
        return this.measurementRepository.findById(p);
    }
}