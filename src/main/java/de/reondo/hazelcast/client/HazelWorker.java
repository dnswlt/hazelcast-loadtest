package de.reondo.hazelcast.client;

import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import static de.reondo.hazelcast.client.App.NANOS_IN_MILLIS;
import static de.reondo.hazelcast.client.StatEntry.Type.READ;
import static de.reondo.hazelcast.client.StatEntry.Type.WRITE;

/**
 * Worker thread Runnable for NOVA Hazelcast load test
 * <p>
 * Created by dwalter on 13.06.2016.
 */
public class HazelWorker implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(HazelWorker.class);

    private static final Queue<String> offerQueue = new LinkedBlockingQueue<>();

    private static final List<StatEntry> statEntries = Collections.synchronizedList(new ArrayList<>());

    private final long durationMillis;
    private final int warmupMillis;
    private final int pauseMillis;
    private final int numBytes;
    private final int numOffers;
    private final double probSale;
    private final HazelcastInstance client;
    private final Random rnd;

    public HazelWorker(long durationMillis, int warmupMillis, int pauseMillis, int numBytes, int numOffers, double probSale, HazelcastInstance client) {
        this.durationMillis = durationMillis;
        this.warmupMillis = warmupMillis;
        this.pauseMillis = pauseMillis;
        this.numBytes = numBytes;
        this.numOffers = numOffers;
        this.probSale = probSale;
        rnd = new Random();
        this.client = client;
    }

    @Override
    public void run() {

        try {
            Map<String, byte[]> warmupMap = client.getMap(App.ANGEBOTE + "-warmup");
            doWarmup(warmupMap);
        } catch (Exception e) {
            LOGGER.error("Warmup failed. Aborting.", e);
            return;
        }
        long delayMillis = rnd.nextInt(1000) + 1;
        LOGGER.debug("Delaying start of timing for {}ms", delayMillis);
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
            LOGGER.error("Ignoring interrupt that occurred during delay.");
        }
        Map<String, byte[]> map = client.getMap(App.ANGEBOTE);
        doTiming(map);
    }

    private void doTiming(Map<String, byte[]> map) {
        LOGGER.info("Starting timing");
        double lambda = (double) numOffers; // lambda of Poisson distribution (= expected value and = variance)
        final double L = Math.exp(-lambda);
        byte[] data = new byte[numBytes];
        long threadId = Thread.currentThread().getId();
        long started = System.nanoTime();
        final long end = started + durationMillis * NANOS_IN_MILLIS;
        long now = started;
        int totalIterations = 0;
        int totalOffers = 0;
        int totalSales = 0;
        final int pauseMs = pauseMillis;
        while (System.nanoTime() < end) {
            ++totalIterations;
            int nOffers = getPoisson(L);
            long before = System.nanoTime();
            for (int i = 0; i < nOffers; ++i) {
                String offerId = generateOffer(map, data);
                ++totalOffers;
                if (i == 0 && (offerQueue.size() < 10000 || rnd.nextDouble() < probSale)) {
                    // Add offer to queue
                    offerQueue.add(offerId);
                }
            }
            long after = System.nanoTime();
            addStat(WRITE, threadId, before - started, after - before, true);
            if (rnd.nextDouble() < probSale) {
                // buy an offer according to look-book ratio
                before = System.nanoTime();
                boolean success = buyOffer(map);
                if (success) {
                    ++totalSales;
                }
                after = System.nanoTime();
                addStat(READ, threadId, before - started, after - before, success);
            }
            if (pauseMs > 0) {
                try {
                    Thread.sleep(pauseMs);
                } catch (InterruptedException e) {
                    LOGGER.warn("Ignoring interrupt while pausing.");
                }
            }
        }
        long durationMillis = (System.nanoTime() - started) / App.NANOS_IN_MILLIS;
        LOGGER.info("Timing done, {} offers created and {} offers bought in {} iterations and {}ms.", totalOffers, totalSales, totalIterations, durationMillis);
    }

    private void addStat(StatEntry.Type type, long threadId, long startNanos, long durationNanos, boolean success) {
        statEntries.add(new StatEntry(type, threadId, success, startNanos, durationNanos));
    }

    /**
     * Take an offer from the offerQueue, and "buy" it, i.e. read it from the map.
     *
     * @param map
     */
    private boolean buyOffer(Map<String, byte[]> map) {
        String key = offerQueue.poll();
        if (key == null) {
            LOGGER.error("Offer queue is empty, load test bug.");
            return false;
        }
        byte[] data = map.get(key);
        if (data == null) {
            LOGGER.debug("Offer for key {} not found in map.", key);
            return false;
        }
        if (data.length != numBytes) {
            LOGGER.error("Offer does not have size. Expected={}, actual={}.", numBytes, data.length);
            return false;
        }
        return true;
    }

    /**
     * Generate random bytes into data, and store with random UUID key in Hazelcast.
     *
     * @param map
     * @param data array to be stored, will be updated with random data
     * @return UUID key
     */
    private String generateOffer(Map<String, byte[]> map, byte[] data) {
        rnd.nextBytes(data);
        String key = UUID.randomUUID().toString();
        map.put(key, data);
        return key;
    }

    public int getPoisson(double L) {
        double p = 1.0;
        int k = 0;
        do {
            k++;
            p *= rnd.nextDouble();
        } while (p > L);

        return k - 1;
    }


    private void doWarmup(Map<String, byte[]> map) {
        long warmupTime = warmupMillis * NANOS_IN_MILLIS;
        long started = System.nanoTime();
        long now = started;
        int count = 0;
        byte[] data = new byte[numBytes];
        long threadId = Thread.currentThread().getId();
        LOGGER.info("Starting warmup for {}ms", warmupTime / 1000000);
        while (now < started + warmupTime) {
            rnd.nextBytes(data);
            map.put(String.format("warmup-%d-%d", threadId, count), data);
            ++count;
            now = System.nanoTime();
        }
        LOGGER.debug("Added {} keys. Finished warmup.", count);
    }

    public static List<StatEntry> getStatEntries() {
        return new ArrayList<>(statEntries);
    }

}
