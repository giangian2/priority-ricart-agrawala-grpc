package smartfab.model.edge;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import smartfab.algorithms.ricart.GrpcPeer;
import smartfab.algorithms.ricart.MutualExclusionAlgorithm;
import smartfab.algorithms.ricart.Peer;
import smartfab.algorithms.ricart.PeerInfo;
import smartfab.algorithms.ricart.RicartMutualExclusionPeer;
import smartfab.model.events.CalibrationGrantEvent;
import smartfab.model.events.CalibrationTerminatedEvent;
import smartfab.model.events.CriticalStatusEvent;
import smartfab.model.events.ProductionLineEvent;
import smartfab.model.events.EventListener;

/**
 * @author Gianluca Bianchi
 *
 *         Production Line class for managing any aspect of the SMARTFAB
 *         production lines.
 */
public class ProductionLine implements EventListener<ProductionLineEvent> {

    public static enum ProductionLineStatus {
        RUNNINING,
        WAITING_FOR_CALIBRATION,
        CALIBRATION
    };

    private final PeerInfo                  peerInfo;
    private final MutualExclusionAlgorithm  algorithm;
    private final Peer                      transport;
    private final MonitoringSensor          sensorThread;
    private final SlidingWindowProcessor    slidingWindowProcessorThread;
    private final AveragesConsumer          averagesConsumerThread;
    private final MeasurementBuffer         measurementBuffer;
    private final AveragesBuffer            averagesBuffer;

    public ProductionLine(int lineId,
            String lineAddress,
            int linePort,
            MutualExclusionAlgorithm algorithm,
            Peer transport,
            MonitoringSensor sensorThread,
            SlidingWindowProcessor slidingWindowProcessor,
            AveragesConsumer averagesConsumerThread,
            MeasurementBuffer measurementBuffer,
            AveragesBuffer averagesBuffer) {

        this.peerInfo                       = new PeerInfo(lineId, lineAddress, linePort);
        this.algorithm                      = algorithm;
        this.transport                      = transport;
        this.sensorThread                   = sensorThread;
        this.slidingWindowProcessorThread   = slidingWindowProcessor;
        this.averagesConsumerThread         = averagesConsumerThread;
        this.measurementBuffer              = measurementBuffer;
        this.averagesBuffer                 = averagesBuffer;
    }

    /**
     * Composition root: creates the transport, registers the known peers and
     * wires the mutual exclusion algorithm on top of it.
     * 
     * @todo
     * WE NEED TO PASS THE RICART MUTUAL EXCLUSION PEER (AND SO ALSO THE GRPC PEER) AS PARAMS TO THIS METHOD.
     * THEY NEEDS TO BE USED BEFORE THE PRODUCTION LINE INITIALIZATION IN ORDER TO START THE GRPC SERVER
     */
    public static Optional<ProductionLine> init(int lineId, String lineAddress, int linePort,
            String adminServerAddress, int adminServerPort, Peer transportPeer) {

        var averagesBuffer          = new AveragesBuffer();
        var measurementBuffer       = new MeasurementBuffer();
        var sensorThread            = new MonitoringSensor(measurementBuffer);
        var slidingWindowProcessor  = new SlidingWindowProcessor(measurementBuffer, averagesBuffer);
        var averagesConsumer        = new AveragesConsumer(lineId, averagesBuffer);
        var algorithm               = new RicartMutualExclusionPeer(transportPeer, lineId);
        var peerRestClient          = new PeerRestClient("http://" + adminServerAddress + ":" + adminServerPort);

        var otherPeers = peerRestClient.registerPeer(new PeerInfo(lineId, lineAddress, linePort));
        otherPeers.forEach(p -> transportPeer.addPeer(new PeerInfo(p.getID(), p.getAddress(), p.getPort())));

        return Optional.of(new ProductionLine(lineId,
                lineAddress,
                linePort,
                algorithm,
                transportPeer,
                sensorThread,
                slidingWindowProcessor,
                averagesConsumer,
                measurementBuffer,
                averagesBuffer));
    }

    public void start() {
        this.startThreads();
    }

    public void pause() {
        this.pauseThreads();
        this.clearBuffers();
    }

    public void stop() {
        this.stopThreads();
        this.clearBuffers();
    }

    public void joinPeerNetwork() {
        this.transport.sendJoinRequestToAll(peerInfo);
    }

    @Override
    public synchronized void onEvent(ProductionLineEvent event) {
        if (event instanceof CriticalStatusEvent) {
            var ev = (CriticalStatusEvent) event;
            this.onCalibrationNeeded(ev.getCriticality());
        }

        if (event instanceof CalibrationGrantEvent) {
            this.onCalibrationAcquired();
        }

        if (event instanceof CalibrationTerminatedEvent) {
            this.onCalibrationTerminated();
        }

        System.out.println("[PRODUCTION LINE] EVENT RECEIVED: " + event.getClass().toString());
    }

    private void clearBuffers() {
        this.averagesBuffer.readAllAndClear();
        this.measurementBuffer.clear();
    }

    private void pauseThreads() {
        this.slidingWindowProcessorThread.stopProcessing();
        this.sensorThread.pauseMeasuring();
        this.averagesConsumerThread.stopConsuming();
    }

    private void startThreads() {
        this.sensorThread.startMeasuring();
        this.slidingWindowProcessorThread.startProcessing();
        this.averagesConsumerThread.startConsuming();
    }

    private void stopThreads() {

    }

    /**
     * Called when the {@link smartfab.model.events.CriticalStatusEvent} is received
     * @param criticality
     */
    private void onCalibrationNeeded(double criticality) {
        this.pause();
        this.algorithm.requestCalibration(criticality);
    }

    /**
     * Called when the {@link smartfab.model.events.CalibrationGrantEvent} is received
     */
    private void onCalibrationAcquired() {
        try {
            System.out.println("LINE " + this.peerInfo.getID() + ": CALIBRATING");
            Thread.sleep(ThreadLocalRandom.current().nextInt(3000, 7001));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        this.onCalibrationTerminated();
    }

    /**
     * Called when the {@link smartfab.model.events.CalibrationTerminatedEvent} is received
     */
    private void onCalibrationTerminated() {
        System.out.println("LINE " + this.peerInfo.getID() + ": Calibration ended");
        this.algorithm.releaseCalibration();
        this.start();
    }

    public static void main(String... args) throws InterruptedException {
        if (args.length < 3) {
            System.err.println("Error: Pass the grpc local line ID, ip address and port [-Pargs='1 127.0.0.1 8001']");
            return;
        }

        int lineId = Integer.parseInt(args[0]);
        String localIp = args[1];
        int localPort = Integer.parseInt(args[2]);
        
        /**
         * WE NEED TO START THE GRPC SERVER BEFORE THE PRODUCTION LINE
         */
        GrpcPeer peer       = new GrpcPeer();    
        var prodLine        = ProductionLine.init(lineId, localIp, localPort, "127.0.0.1", 8080, peer);


        if (prodLine.isPresent()) {
            ProductionLine pl = prodLine.get();

            pl.slidingWindowProcessorThread.subscribe(pl);
            pl.algorithm.subscribe(pl);

            Server grpcServer   = ServerBuilder.forPort(localPort)
                .addService(new CalibrationServiceImpl(pl.algorithm))
                .build();

            try {
                grpcServer.start();
                System.out.println("[gRPC SERVER] Listening on port: " + localPort + "...");

            } catch (IOException e) {
                System.err.println("Can't start grpc server at port:  " + localPort);
                e.printStackTrace();
                return;
            }

            pl.joinPeerNetwork();
            pl.start();

            while (true) {
                Thread.sleep(20000);
            }

        } else {
            System.out.println("Error initiating production line");
        }
    }
}
