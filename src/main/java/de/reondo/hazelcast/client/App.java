package de.reondo.hazelcast.client;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;


/**
 * Created by dwalter on 10.06.2016.
 */
public class App {

    public static final String ANGEBOTE = "angebote";

    public static final long NANOS_IN_MILLIS = 1000000;

    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        new App().run(args);
    }

    private void run(String[] args) {
        System.setProperty("hazelcast.logging.type", "slf4j");

        OptionParser parser = new OptionParser();
        parser.accepts("d", "Timing duration in seconds").withRequiredArg().ofType(Integer.class).required();
        parser.accepts("b", "Bytes per offer").withRequiredArg().ofType(Integer.class).defaultsTo(4000);
        parser.accepts("n", "Avg. number of offers per iteration").withRequiredArg().ofType(Integer.class).defaultsTo(8);
        parser.accepts("s", "Probability of buying an offer per iteration").withRequiredArg().ofType(Double.class).defaultsTo(0.2);
        parser.accepts("t", "Number of threads").withRequiredArg().ofType(Integer.class).defaultsTo(1);
        parser.accepts("h", "Comma-separated list of Hazelcast hostnames").withRequiredArg().ofType(String.class).defaultsTo("localhost");
        parser.accepts("w", "Warmup time in seconds").withRequiredArg().ofType(Integer.class).defaultsTo(5);
        parser.accepts("p", "Pause in milliseconds between two iterations").withRequiredArg().ofType(Integer.class).defaultsTo(0);
        parser.accepts("help", "Print help").forHelp();
        OptionSet options;
        try {
            options = parser.parse(args);
        } catch (OptionException e) {
            printHelpAndExit(parser);
            return; // never reached
        }
        if (options.has("help") ) {
            printHelpAndExit(parser);
        }

        int durationMillis = (int) options.valueOf("d") * 1000;
        int numBytes = (int) options.valueOf("b");
        int numOffers = (int) options.valueOf("n");
        double probSale = (double) options.valueOf("s");
        int numThreads = (int) options.valueOf("t");
        int warmupMillis = (int) options.valueOf("w") * 1000;
        int pauseMillis = (int) options.valueOf("p");
        List<String> hazelcastHosts = Arrays.asList(((String)options.valueOf("h")).split(","));

        LOGGER.info("Starting test with config:\n  threads={}, duration={}s, pause={}ms, bytes-per-key={}, offers-per-iter={}, " +
                "look-book-ratio={}%, hazelcast-hosts={}", numThreads, durationMillis/1000, pauseMillis, numBytes, numOffers,
                String.format(Locale.US, "%.2f", probSale * 100.0), hazelcastHosts);

        Thread[] ts = new Thread[numThreads];
        HazelWorker[] workers = new HazelWorker[ts.length];
        // Create threads
        HazelcastInstance client = createHazelcastClient(hazelcastHosts);
        LOGGER.info("Clearing map (size={})", client.getMap(ANGEBOTE).size());
        client.getMap(ANGEBOTE).clear();
        long startTime;
        try {
            for (int i = 0; i < ts.length; ++i) {
                workers[i] = new HazelWorker(durationMillis, warmupMillis, pauseMillis, numBytes, numOffers, probSale, client);
                ts[i] = new Thread(workers[i]);
            }
            startTime = System.nanoTime();
            // Start threads
            for (int i = 0; i < ts.length; ++i) {
                ts[i].start();
            }
            // Join threads
            for (int i = 0; i < ts.length; ++i) {
                try {
                    ts[i].join();
                } catch (InterruptedException e) {
                    LOGGER.error("Interrupted while joining thread {}", ts[i].getName());
                }
            }
            long endTime = System.nanoTime();
            LOGGER.info("Test finished after {}ms. Map has {} entries.", (endTime - startTime) / NANOS_IN_MILLIS,
                    client.getMap(ANGEBOTE).size());
            printStats();
        } finally {
            if (client != null) {
                client.shutdown(); // ensure JVM will terminate
            }
        }
    }

    private void printHelpAndExit(OptionParser parser) {
        try {
            parser.printHelpOn(System.out);
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void printStats() {
        List<StatEntry> entries = HazelWorker.getStatEntries();
        StatSummary summary = StatSummary.builder(entries).withSuccessOnly(true).build();
        LOGGER.info("Load test statistics:\n" +
                "  successful number of reads: {}\n" +
                "  read success rate: {}%\n" +
                "  read throughput (req/s): {}\n" +
                "  read 99.9 percentile (ms): {}\n" +
                "  read 99 percentile (ms): {}\n" +
                "  read 95 percentile (ms): {}\n" +
                "  read mean (ms): {}\n" +
                "  successful number of writes: {}\n" +
                "  write throughput (req/s): {}\n" +
                "  write 99.9 percentile (ms): {}\n" +
                "  write 99 percentile (ms): {}\n" +
                "  write 95 percentile (ms): {}\n" +
                "  write mean (ms): {}",
                summary.getReadCount(),
                String.format(Locale.US, "%.2f", summary.getReadSuccessRate() * 100.0),
                summary.getReadThroughput(),
                (double)summary.getReadPercentile(0.999)/NANOS_IN_MILLIS,
                (double)summary.getReadPercentile(0.99)/NANOS_IN_MILLIS,
                (double)summary.getReadPercentile(0.95)/NANOS_IN_MILLIS,
                String.format(Locale.US, "%.2f", summary.getReadMeanMillis()),
                summary.getWriteCount(),
                summary.getWriteThroughput(),
                (double)summary.getWritePercentile(0.999)/NANOS_IN_MILLIS,
                (double)summary.getWritePercentile(0.99)/NANOS_IN_MILLIS,
                (double)summary.getWritePercentile(0.95)/NANOS_IN_MILLIS,
                String.format(Locale.US, "%.2f", summary.getWriteMeanMillis()));
        File resultFile = new File(String.format("timings_%s.csv",
                new SimpleDateFormat("yyyy-MM-dd'T'HHmmss").format(new Date())));
        summary.saveEntries(resultFile);
    }


    private HazelcastInstance createHazelcastClient(List<String> hazelcastHosts) {
        ClientConfig clientConfig = new ClientConfig();
        // Only works as system property, not if set as follows o_O
        // clientConfig.setProperty("hazelcast.logging.type", "none");
        ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
        clientNetworkConfig.setAddresses(hazelcastHosts);
        clientConfig.setNetworkConfig(clientNetworkConfig);
        return HazelcastClient.newHazelcastClient(clientConfig);
    }


}
