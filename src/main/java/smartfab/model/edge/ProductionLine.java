package smartfab.model.edge;

import java.util.Optional;

import smartfab.algorithms.ricart.GrpcPeer;
import smartfab.algorithms.ricart.MutualExclusionAlgorithm;
import smartfab.algorithms.ricart.RicartMutualExclusionPeer;
import smartfab.model.events.CalibrationGrantEvent;
import smartfab.model.events.CalibrationTerminatedEvent;
import smartfab.model.events.CriticalStatusEvent;
import smartfab.model.events.EventListener;
import smartfab.model.events.ProductionLineEvent;


/**
 * @author Gianluca Bianchi
 * 
 * Production Line class for manage any aspect of the SMARTFAB production lines.
 * This class needs: 
 *      {@link smartfab.algorithms.ricart.MutualExclusionAlgorithm}
 *      {@link smartfab.model.edge.MonitoringSensor}
 *      {@link smartfab.model.edge.SlidingWindowProcessor}
 *      {@link smartfab.model.edge.AveragesConsumer}
 *      {@link smartfab.model.edge.AveragesBuffer}
 *      {@link smartfab.model.edge.MeasurementBuffer}
 */
public class ProductionLine implements EventListener<ProductionLineEvent>{

    public static enum ProductionLineStatus{
        RUNNINING,
        WAITING_FOR_CALIBRATION,
        CALIBRATION
    };
    
    private final int                       lineId;

    private final MutualExclusionAlgorithm  mutualExclusionPeer;
    private final MonitoringSensor          sensorThread;
    private final SlidingWindowProcessor    slidingWindowProcessorThread;
    private final AveragesConsumer          averagesConsumerThread;

    private final MeasurementBuffer         measurementBuffer;
    private final AveragesBuffer            averagesBuffer;

    public ProductionLine(int lineId,
                            MutualExclusionAlgorithm mutualExclusionPeer,
                            MonitoringSensor sensorThread,
                            SlidingWindowProcessor slidingWindowProcessor,
                            AveragesConsumer averagesConsumerThread,
                            MeasurementBuffer measurementBuffer,
                            AveragesBuffer  averagesBuffer){

        this.lineId                         = lineId;
        this.mutualExclusionPeer            = mutualExclusionPeer;
        this.sensorThread                   = sensorThread;
        this.slidingWindowProcessorThread   = slidingWindowProcessor;
        this.averagesConsumerThread         = averagesConsumerThread;
        this.measurementBuffer              = measurementBuffer;
        this.averagesBuffer                 = averagesBuffer;
    }

    /**
     * This method will perform the setup of the production line starting by creating the following resources:
     *  - Buffers for storing measurements and agreggated-data
     *  - Threads for sensor and for data-acquisition pipline and mqtt pub
     * 
     * In order to configure the {@link smartfab.algorithms.ricart.GrpcPeer} we need to call
     * the administration server with address and port retreived from input.
     * 
     * The administration server will return a list of other peers in the P2P system and these connection
     * will be added to the current peer instance.
     * @param lineId
     * @param adminServerAddress
     * @param adminServerPort
     * @return 
     */
    public static Optional<ProductionLine> init(int lineId, String adminServerAddress, int adminServerPort){
        //Initialize shared buffers
        var averagesBuffer          = new AveragesBuffer();
        var measurementBuffer       = new MeasurementBuffer();
        //Initialize core threads (needs buffers already created)
        var sensorThread            = new MonitoringSensor(measurementBuffer);
        var slidingWindowProcessor  = new SlidingWindowProcessor(measurementBuffer, averagesBuffer);
        var AveragesConsumer        = new AveragesConsumer(lineId, averagesBuffer);
        var peer                    = new RicartMutualExclusionPeer(new GrpcPeer());
        
        /**
         * @TODO
         * Make HTTP request to administration server in order to get the peer list and to the
         * peer.addPeer() operation for each one.
         * 
         */

        return Optional.of(new ProductionLine(lineId,
            peer, 
            sensorThread, 
            slidingWindowProcessor, 
            AveragesConsumer, 
            measurementBuffer, 
            averagesBuffer));
    }

    /**
     * This method starts all the core threads (sensor, sliding window and mqtt publisher)
     * and the {@link smartfab.model.edge.CalibrationServiceImpl} grpc instance.
     */
    public void start(){
        this.startThreads();
    }

    public void pause(){
        this.pauseThreads();
        this.clearBuffers();
    }

    public void stop(){
        this.stopThreads();
        this.clearBuffers();
    }
    
    
    @Override
    public void onEvent(ProductionLineEvent event) {
        if(event instanceof CriticalStatusEvent){
            var ev = (CriticalStatusEvent) event;
            this.onCalibrationNeeded(ev.getCriticality());
        }

        if(event instanceof CalibrationGrantEvent){
            var ev = (CalibrationGrantEvent) event;
            this.onCalibrationAcquired();
        }

        if(event instanceof CalibrationTerminatedEvent){
            var ev = (CalibrationTerminatedEvent) event;
            this.onCalibrationTerminated();
        }

        System.out.println("[PRODUCTION LINE] EVENT RECEIVED: "+event.getClass().toString());
    }

    private void clearBuffers(){
        this.averagesBuffer.readAllAndClear();
        this.measurementBuffer.clear();
    }

    private void pauseThreads(){
        this.slidingWindowProcessorThread.stopProcessing();
        this.sensorThread.pauseMeasuring();
    }

    private void startThreads(){
        this.sensorThread.startMeasuring();
        this.slidingWindowProcessorThread.startProcessing();
        this.averagesConsumerThread.start();
    }

    private void stopThreads(){

    }

    private void onCalibrationNeeded(int criticality){
        this.pause();
        this.mutualExclusionPeer.requestCalibration(lineId, criticality);
    }

    private void onCalibrationAcquired(){
        try {
            Thread.sleep(6000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        this.onCalibrationTerminated();
    }

    private void onCalibrationTerminated(){
        this.start();
    }

    public static void main(String... args) throws InterruptedException{
        var prodLine = ProductionLine.init(0, "127.0.0.1", 8000);

        if(prodLine.isPresent()){
            prodLine.get().start();

            Thread.sleep(20000);

            prodLine.get().pause();

            Thread.sleep(2000);

            prodLine.get().start();

            Thread.sleep(10000);

            prodLine.get().stop();
        }else{
            System.out.println("Error initiating production line");
        }
    }
}