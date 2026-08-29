package smartfab.model.edge;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import smartfab.algorithms.ricart.MutualExclusionAlgorithm;
import smartfab.algorithms.ricart.PeerInfo;
import smartfab.algorithms.ricart.RicartEngine;
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

    private static final class ProductionLineBuilder {
        private PeerInfo                  peerInfo;
        private MutualExclusionAlgorithm  algorithm;
        private MonitoringSensor          sensorThread;
        private SlidingWindowProcessor    slidingWindowProcessorThread;
        private AveragesConsumer          averagesConsumerThread;
        private MeasurementBuffer         measurementBuffer;
        private AveragesBuffer            averagesBuffer;

        ProductionLineBuilder peerInfo(int lineId, String lineAddress, int linePort) {
            this.peerInfo = new PeerInfo(lineId, lineAddress, linePort);
            return this;
        }

        ProductionLineBuilder algorithm(MutualExclusionAlgorithm algorithm) {
            this.algorithm = algorithm;
            return this;
        }

        ProductionLineBuilder sensorThread(MonitoringSensor sensorThread) {
            this.sensorThread = sensorThread;
            return this;
        }

        ProductionLineBuilder slidingWindowProcessorThread(SlidingWindowProcessor slidingWindowProcessorThread) {
            this.slidingWindowProcessorThread = slidingWindowProcessorThread;
            return this;
        }

        ProductionLineBuilder averagesConsumerThread(AveragesConsumer averagesConsumerThread) {
            this.averagesConsumerThread = averagesConsumerThread;
            return this;
        }

        ProductionLineBuilder measurementBuffer(MeasurementBuffer measurementBuffer) {
            this.measurementBuffer = measurementBuffer;
            return this;
        }

        ProductionLineBuilder averagesBuffer(AveragesBuffer averagesBuffer) {
            this.averagesBuffer = averagesBuffer;
            return this;
        }

        ProductionLine build() {
            return new ProductionLine(this);
        }
    }

    public static ProductionLineBuilder builder() {
        return new ProductionLineBuilder();
    }

    public static enum ProductionLineStatus {
        RUNNINING,
        WAITING_FOR_CALIBRATION,
        CALIBRATION
    };

    private final PeerInfo                  peerInfo;
    private final MutualExclusionAlgorithm  algorithm;
    private final MonitoringSensor          sensorThread;
    private final SlidingWindowProcessor    slidingWindowProcessorThread;
    private final AveragesConsumer          averagesConsumerThread;
    private final MeasurementBuffer         measurementBuffer;
    private final AveragesBuffer            averagesBuffer;

    private ProductionLine(ProductionLineBuilder builder) {
        this.peerInfo                       = builder.peerInfo;
        this.algorithm                      = builder.algorithm;
        this.sensorThread                   = builder.sensorThread;
        this.slidingWindowProcessorThread   = builder.slidingWindowProcessorThread;
        this.averagesConsumerThread         = builder.averagesConsumerThread;
        this.measurementBuffer              = builder.measurementBuffer;
        this.averagesBuffer                 = builder.averagesBuffer;
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
        this.slidingWindowProcessorThread.interrupt();
        this.averagesConsumerThread.stopConsuming();
        this.averagesConsumerThread.interrupt();
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

    /** How long the join handshake waits for the acknowledgements. */
    private static final long JOIN_TIMEOUT_MILLIS = 5000;

    public static void main(String... args) throws InterruptedException {
        if (args.length < 4) {
            System.err.println("Error: Pass the grpc local line ID, ip address, port and server url [-Pargs='1 127.0.0.1 8001 http://localhost::8080']");
            return;
        }

        int lineId          = Integer.parseInt(args[0]);
        String localIp      = args[1];
        int localPort       = Integer.parseInt(args[2]);
        String serverUrl    = args[3];

        var self                    = new PeerInfo(lineId, localIp, localPort);
        var transport               = new GrpcPeerTransport();
        var algorithm               = new RicartEngine(transport, self);

        var averagesBuffer          = new AveragesBuffer();
        var measurementBuffer       = new MeasurementBuffer();
        var sensorThread            = new MonitoringSensor(measurementBuffer);
        var slidingWindowProcessor  = new SlidingWindowProcessor(measurementBuffer, averagesBuffer);
        var averagesConsumer        = new AveragesConsumer(lineId, averagesBuffer);
        var peerRestClient          = new PeerRestClient(serverUrl);

        ProductionLine pl = ProductionLine.builder()
                .peerInfo(lineId, localIp, localPort)
                .algorithm(algorithm)
                .sensorThread(sensorThread)
                .slidingWindowProcessorThread(slidingWindowProcessor)
                .averagesConsumerThread(averagesConsumer)
                .measurementBuffer(measurementBuffer)
                .averagesBuffer(averagesBuffer)
                .build();

        pl.slidingWindowProcessorThread.subscribe(pl);
        algorithm.subscribe(pl);
        algorithm.subscribe(new MqttStateListener());

        Server grpcServer = ServerBuilder.forPort(localPort)
                .addService(new CalibrationServiceImpl(algorithm))
                .build();

        try {
            /*
             * BOOTSTRAP ORDER, and every step depends on the previous one:
             *
             *  1. listen  - registering first would advertise an address that
             *               refuses connections until the server is up;
             *  2. register - atomic on the admin server, so two lines starting
             *               together cannot both receive a list that excludes
             *               the other and both believe they are alone;
             *  3. join    - only the peers that acknowledge enter the topology,
             *               so a peer listed but already dead can never make
             *               the quorum unreachable;
             *  4. start   - only now can a sensor trigger a calibration:
             *               before the join the quorum would be empty and the
             *               line would enter the section on its own.
             */
            grpcServer.start();
            System.out.println("[gRPC SERVER] Listening on port: " + localPort + "...");

            List<PeerInfo> known = peerRestClient.registerPeer(self);
            System.out.println("REGISTERED, " + known.size() + " peer(s) already in the network");

            if (!algorithm.join(known, JOIN_TIMEOUT_MILLIS)) {
                System.out.println("WARNING: some peers did not acknowledge the join, proceeding without them");
            }

            pl.start();

            boolean running = true;
            var inputStreamReader = new BufferedReader(new InputStreamReader(System.in));

            while (running) {
                String inputCommand = inputStreamReader.readLine();
                if (inputCommand == null || inputCommand.equals("exit") || inputCommand.equals("quit")) {
                    System.out.println("Received STOP command!");
                    running = false;
                }
            }

            /*
             * Deregister BEFORE announcing the exit to the peers: since
             * registration and removal share the admin server monitor, a line
             * joining at this instant cannot receive our address any more.
             */
            peerRestClient.removePeer(self);
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

            //After all threads finished, close Mqtt client
            MqttClientManager.getInstance().disconnect();
            System.out.println("MQTT client disconnected!");
            System.exit(0);

        } catch (IOException e) {
            System.err.println("Can't start grpc server at port:  " + localPort);
            e.printStackTrace();
        }
    }
}
