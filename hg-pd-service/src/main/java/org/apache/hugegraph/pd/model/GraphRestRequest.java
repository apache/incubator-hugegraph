package org.apache.hugegraph.pd.model;

import lombok.Data;

@Data
public class GraphRestRequest {
    private int partitionCount;
    private int shardCount;
}
