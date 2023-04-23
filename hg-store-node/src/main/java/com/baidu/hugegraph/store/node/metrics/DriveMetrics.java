package com.baidu.hugegraph.store.node.metrics;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lynn.bond@hotmail.com on 2021/11/23
 */
@Deprecated
public class DriveMetrics {

    private static long MIB = 1024 * 1024;
    // TODO: add a cache
    public Map<String, Map<String, Object>> metrics() {
        File[] rootDrive = File.listRoots();

        if (rootDrive == null) {
            return new LinkedHashMap(0);
        }

        Map<String, Map<String, Object>> metrics = new HashMap<>();

        for (File d : rootDrive) {
            Map<String, Object> buf = new HashMap<>();
            buf.put("total_space", d.getTotalSpace() / MIB);
            buf.put("free_space", d.getFreeSpace() / MIB);
            buf.put("usable_space", d.getUsableSpace() / MIB);
            buf.put("size_unit", "MB");

            metrics.put(d.getPath().replace("\\",""), buf);

        }

        return metrics;

    }

}
