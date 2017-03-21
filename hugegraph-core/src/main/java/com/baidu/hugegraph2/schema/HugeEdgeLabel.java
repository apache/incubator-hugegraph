package com.baidu.hugegraph2.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.baidu.hugegraph2.Cardinality;
import com.baidu.hugegraph2.Multiplicity;
import com.baidu.hugegraph2.schema.base.EdgeLabel;

/**
 * Created by liningrui on 2017/3/20.
 */
public class HugeEdgeLabel implements EdgeLabel {

    private String name;
    // multiplicity：图的角度，描述多个顶点之间的关系。多对一，多对多，一对多，一对一
    private Multiplicity multiplicity;
    // cardinality：两个顶点之间是否可以有多条边
    private Cardinality cardinality;

    private HugeVertexLabel srcVertexLabel;
    private HugeVertexLabel tgtVertexLabel;

    // 指定的分区键列表
    private List<String> partitionKeys;

    public HugeEdgeLabel(String name) {
        this.name = name;
        this.multiplicity = Multiplicity.ONE2ONE;
        this.cardinality = Cardinality.SINGLE;
    }

    @Override
    public Multiplicity multiplicity() {
        return multiplicity;
    }

    public void setMultiplicity(Multiplicity multiplicity) {
        this.multiplicity = multiplicity;
    }

    @Override
    public Cardinality cardinality() {
        return cardinality;
    }

    public void setCardinality(Cardinality cardinality) {
        this.cardinality = cardinality;
    }

    @Override
    public boolean isDirected() {
        return false;
    }

    @Override
    public boolean hasPartitionKeys() {
        return partitionKeys != null && !partitionKeys.isEmpty();
    }

    @Override
    public String schema() {
        return null;
    }

    @Override
    public String name() {
        return name;
    }

    public void connection(String fromVertexLabel, String toVertexLabel) {
        this.srcVertexLabel = new HugeVertexLabel(fromVertexLabel);
        this.tgtVertexLabel = new HugeVertexLabel(toVertexLabel);
    }

    public void addPartitionKeys(String... keys) {
        if (partitionKeys == null) {
            partitionKeys = new ArrayList<>();
        }
        partitionKeys.addAll(Arrays.asList(keys));
    }
}
