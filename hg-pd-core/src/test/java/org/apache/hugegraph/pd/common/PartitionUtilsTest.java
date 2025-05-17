package org.apache.hugegraph.pd.common;

// import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class PartitionUtilsTest {

    // @Test
    public void testHashCode() {
        int partCount = 10;
        int partSize = PartitionUtils.MAX_VALUE / partCount+1;
        int[] counter = new int[partCount];
        for (int i = 0; i < 10000; i++) {
            String s = String.format("BATCH-GET-UNIT-%02d", i);
            int c = PartitionUtils.calcHashcode(s.getBytes(StandardCharsets.UTF_8));

            counter[c / partSize]++;

        }

        for (int i = 0; i < counter.length; i++)
            System.out.println(i + " " + counter[i]);
    }


}
