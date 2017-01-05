package de.reondo.hazelcast.client;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;
import org.w3c.dom.ranges.Range;

/**
 * Created by dwalter on 05.01.2017.
 */
public class HazelWorkerTest {

    @Test
    public void testPoisson() {
        HazelWorker hw = new HazelWorker(new ConfigBuilder().createConfig(), null, null);
        double lambda = (double) 12; // lambda of Poisson distribution (= expected value and = variance)
        final double L = Math.exp(-lambda);
        int maxN = 0;
        for (int i = 0; i < 10000; i++) {
            maxN = Math.max(maxN, hw.getPoisson(L));
        }
        assertTrue("maxN too large: " + maxN, maxN < 3 * lambda);
    }

    @Test
    public void testNextBytes() {
        Random rnd = new Random();
        byte[] data = new byte[18000];
        long before = System.nanoTime();
        for (int i = 0; i < 100000; i++) {
            rnd.nextBytes(data);
        }
        long after = System.nanoTime();
        System.out.println("Took " + ((after-before)/1_000_000L) + "ms");
    }
}