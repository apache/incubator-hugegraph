package com.baidu.hugegraph.store.node.metrics;

import com.baidu.hugegraph.store.node.util.HgRegexUtil;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


/**
 * @author lynn.bond@hotmail.com on 2022/3/7
 */
public class JraftMetricsTest {

    @Test
    public void testRefineLabel() {
        String regex = "(replicator)(.+?:\\d+)(.*)";

        String[] sources = {
                "replicator_10.14.139.10:8081_append_entries_times",
                "replicator10.14.139.10:8081appendentriestimes",
                "replicator.10.14.139.10:8081.append.entries.times",
                "replicator-10-14-139-10:8081-append-entries-times",
                "replicator_hg_0_10_14_139_10:8081::100_replicate_inflights_count_min",
                "replicasdf14-13dasfies-times",
        };


        Arrays.stream(sources).forEach(e -> {
            System.out.println("--- "+e+" ---");
            List<String> list = HgRegexUtil.toGroupValues(regex, e);
            if (list != null) {
                list.forEach(System.out::println);
            }else{
                System.out.println("NONE");
            }
            System.out.println("");
        });

    }

}
