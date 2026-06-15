package smartfab.http.respository;

import org.springframework.stereotype.Repository;

import smartfab.model.edge.Measurement;

@Repository
public class MeasurementRepository extends AbstractInMemoryRepository<String, Measurement> {}