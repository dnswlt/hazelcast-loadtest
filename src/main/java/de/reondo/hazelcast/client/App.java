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

import java.io.IOException;
import java.util.Arrays;
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
        parser.accepts("l", "Disable throttling in case of failed reads");
        parser.accepts("c", "Clear map before start of test");
        parser.accepts("dummy", "Disable smart routing (dummy client)");
        parser.accepts("redo", "Enable redo-able operations");
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

        ConfigBuilder cfg = new ConfigBuilder();
        cfg.setDurationMillis((Integer) options.valueOf("d") * 1000L);
        cfg.setNumBytes((Integer) options.valueOf("b"));
        cfg.setNumOffers((Integer) options.valueOf("n"));
        cfg.setProbSale((Double) options.valueOf("s"));
        cfg.setNumThreads((Integer) options.valueOf("t"));
        cfg.setWarmupMillis((Integer) options.valueOf("w") * 1000L);
        cfg.setPauseMillis((Integer) options.valueOf("p") * 1L);
        cfg.setThrottlingEnabled(!options.has("l"));
        cfg.setClearMap(options.has("c"));
        List<String> hazelcastHosts = Arrays.asList(((String)options.valueOf("h")).split(","));
        cfg.setHazelcastHosts(hazelcastHosts);

        Config config = cfg.createConfig();
        LOGGER.info("Starting test with config:\n  {}", config);

        Thread[] ts = new Thread[config.getNumThreads()];
        HazelWorker[] workers = new HazelWorker[ts.length];
        // Create threads
        HazelcastInstance client = createHazelcastClient(config.getHazelcastHosts(), options.has("dummy"), options.has("redo"));

        if (config.isClearMap()) {
            LOGGER.info("Clearing map (size={})", client.getMap(ANGEBOTE).size());
            client.getMap(ANGEBOTE).clear();
        }
        try {
            for (int i = 0; i < ts.length; ++i) {
                workers[i] = new HazelWorker(config, client);
                ts[i] = new Thread(workers[i]);
            }
            long startTime = System.nanoTime();
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
        summary.saveEntries();
    }

    private HazelcastInstance createHazelcastClient(List<String> hazelcastHosts, boolean isDummy, boolean isRedo) {
        ClientConfig clientConfig = new ClientConfig();
        // Only works as system property, not if set as follows o_O
        // clientConfig.setProperty("hazelcast.logging.type", "none");
        ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
        clientNetworkConfig.setAddresses(hazelcastHosts);
        if (isDummy) {
            LOGGER.info("Disabling smart routing (dummy client)");
            clientNetworkConfig.setSmartRouting(false);
        }
        if (isRedo) {
            LOGGER.info("Enabling redo-operation");
            clientNetworkConfig.setRedoOperation(true);
        }
        clientConfig.setNetworkConfig(clientNetworkConfig);
        return HazelcastClient.newHazelcastClient(clientConfig);
    }


}
