package smartfab.http.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import smartfab.http.respository.LinesMeasurementRepository;
import smartfab.model.mqtt.AverageMessage;

import java.util.Collections;
import java.util.List;

@Service
public class MeasurementService {

    @Autowired
    private LinesMeasurementRepository measurementRepository;

    public void addMeasurement(int p, AverageMessage m){
        this.measurementRepository.save(p, Collections.singletonList(m));
    }

    public List<AverageMessage> getLineMeasurements(int p){
        return this.measurementRepository.findById(p);
    }
}