package smartfab.model.edge;

public class ProductionLineMain {
    

    public static void main(String... args) throws InterruptedException{

        var measurementBuffer   = new MeasurementBuffer();
        var averagesBuffer      = new AveragesBuffer();
        var sensorThread        = new MonitoringSensor(measurementBuffer);
        var windowThread        = new SlidingWindowProcessor(measurementBuffer, averagesBuffer);
        var pubThread           = new AveragesConsumer(10, averagesBuffer);

        sensorThread.startMeasuring();
        windowThread.startProcessing();
        pubThread.start();

        Thread.sleep(20000);

        sensorThread.pauseMeasuring();

        System.out.println("Stopped measuring");

        Thread.sleep(5000);

        sensorThread.startMeasuring();

        System.out.println("Restarted measuring");

        while(true){
            Thread.sleep(20000);
        }
        
    }
}
