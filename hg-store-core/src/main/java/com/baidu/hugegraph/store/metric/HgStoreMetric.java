package com.baidu.hugegraph.store.metric;

import lombok.Data;

import java.util.List;

public class HgStoreMetric {

    @Data
    public static class Table {
        private String tableName;
        private long keyCount;
        private String dataSize;
    }

    @Data
    public static class Partition {
        private int partitionId;
        private List<Table> tables;
    }

    @Data
    public static class Graph {
        private long approxDataSize;
        private long approxKeyCount;
    }
}
