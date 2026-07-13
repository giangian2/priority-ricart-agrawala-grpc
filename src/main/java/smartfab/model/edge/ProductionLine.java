package smartfab.model.edge;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import smartfab.model.mqtt.MqttClientManager;
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
     * Creates the transport, registers the known peers and
     * wires the mutual exclusion algorithm on top of it.
     */
    public static Optional<ProductionLine> init(int lineId, String lineAddress, int linePort,
            Peer transportPeer,MutualExclusionAlgorithm algorithm, AveragesBuffer averagesBuffer, 
            MeasurementBuffer measurementBuffer, MonitoringSensor sensorThread, PeerRestClient restClient,
            SlidingWindowProcessor slidingWindowProcessor, AveragesConsumer averagesConsumer) {


        var otherPeers = restClient.registerPeer(new PeerInfo(lineId, lineAddress, linePort));
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
        this.algorithm.shutdown();
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
    }

    private void clearBuffers() {
        this.averagesBuffer.readAllAndClear();
        this.measurementBuffer.clear();
    }

    private void pauseThreads() {
        this.slidingWindowProcessorThread.pauseProcessing();
        this.sensorThread.pauseMeasuring();
        this.averagesConsumerThread.pauseConsuming();
    }

    private void startThreads() {
        this.sensorThread.startMeasuring();
        this.slidingWindowProcessorThread.startProcessing();
        this.averagesConsumerThread.startConsuming();
    }

    private void stopThreads() {
        this.sensorThread.stopMeasuring();
        this.slidingWindowProcessorThread.stopProcessing();
        this.averagesConsumerThread.stopConsuming();
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
        if (args.length < 4) {
            System.err.println("Error: Pass the grpc local line ID, ip address, port and server url [-Pargs='1 127.0.0.1 8001 http://localhost::8080']");
            return;
        }

        int lineId          = Integer.parseInt(args[0]);
        String localIp      = args[1];
        int localPort       = Integer.parseInt(args[2]);
        String serverUrl    = args[3];
        
        /**
         * WE NEED TO START THE GRPC SERVER BEFORE THE PRODUCTION LINE
         */
        var peer                    = new GrpcPeer();    
        var averagesBuffer          = new AveragesBuffer();
        var measurementBuffer       = new MeasurementBuffer();
        var sensorThread            = new MonitoringSensor(measurementBuffer);
        var slidingWindowProcessor  = new SlidingWindowProcessor(measurementBuffer, averagesBuffer);
        var averagesConsumer        = new AveragesConsumer(lineId, averagesBuffer);
        var algorithm               = new RicartMutualExclusionPeer(peer, lineId);
        var peerRestClient          = new PeerRestClient(serverUrl);



        var prodLine = ProductionLine.init(
                lineId, 
                localIp, 
                localPort, 
                peer, 
                algorithm,
                averagesBuffer, 
                measurementBuffer, 
                sensorThread, 
                peerRestClient,
                slidingWindowProcessor, 
                averagesConsumer);


        if (prodLine.isPresent()) {
            ProductionLine pl = prodLine.get();

            pl.slidingWindowProcessorThread.subscribe(pl);
            pl.algorithm.subscribe(pl);

            Server grpcServer   = ServerBuilder.forPort(localPort)
                .addService(new CalibrationServiceImpl(pl.algorithm))
                .build();

            try {
                boolean running = true;
                grpcServer.start();
                System.out.println("[gRPC SERVER] Listening on port: " + localPort + "...");
                
                pl.joinPeerNetwork();
                pl.start();

                var inputStreamReader =  new BufferedReader(new InputStreamReader(System.in));

                while(running){
                    String inputCommand = inputStreamReader.readLine();
                    if(inputCommand.equals("exit") || inputCommand.equals("quit")){
                        System.out.println("Received STOP command!");
                        running=false;
                    }
                }

                /**
                 * Before sending EXIT message, notify the admin server.
                 * In this way it's not possible to reach the inconsistent state when
                 * a line exit and another join in the same time: since register and remove 
                 * are synchornized on the server, it can't happen that the new registering
                 * line will receive the exited peer address and port.
                 */
                peerRestClient.removePeer(new PeerInfo(lineId, localIp, localPort));
                pl.stop();
                            
                //Wait all threads to finish
                measurementBuffer.unblock();
                averagesConsumer.join();
                sensorThread.join();
                slidingWindowProcessor.join();
                System.out.println("Processors threads stopped!");
                grpcServer.shutdown();
                if (!grpcServer.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    grpcServer.shutdownNow();
                    grpcServer.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
                }
                System.out.println("gRPC server stopped!");
                //After all threds finished, close Mqtt client
                MqttClientManager.getInstance().disconnect();
                System.out.println("MQTT client disconnected!");
                System.exit(0);
            } catch (IOException e) {
                System.err.println("Can't start grpc server at port:  " + localPort);
                e.printStackTrace();
                return;
            }

        } else {
            System.out.println("Error initiating production line");
        }
    }
}
