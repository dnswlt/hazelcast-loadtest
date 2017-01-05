package de.reondo.hazelcast.client;

import static org.junit.Assert.*;

import org.junit.Test;

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
}