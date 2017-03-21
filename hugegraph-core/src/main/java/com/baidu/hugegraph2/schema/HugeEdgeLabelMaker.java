package com.baidu.hugegraph2.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.Cardinality;
import com.baidu.hugegraph2.Multiplicity;
import com.baidu.hugegraph2.backend.store.SchemaStore;
import com.baidu.hugegraph2.schema.base.SchemaType;
import com.baidu.hugegraph2.schema.base.maker.EdgeLabelMaker;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugeEdgeLabelMaker implements EdgeLabelMaker {

    private static final Logger logger = LoggerFactory.getLogger(HugeEdgeLabelMaker.class);

    private SchemaStore schemaStore;
    private HugeEdgeLabel edgeLabel;
    private String name;

    private List<String> partitionKeys;

    public HugeEdgeLabelMaker(SchemaStore schemaStore, String name) {
        this.name = name;
        this.schemaStore = schemaStore;
        edgeLabel = new HugeEdgeLabel(name);
    }

    @Override
    public EdgeLabelMaker connection(String fromVertexLabel, String toVertexLabel) {
        this.edgeLabel.connection(fromVertexLabel, toVertexLabel);
        return this;
    }

    @Override
    public EdgeLabelMaker single() {
        this.edgeLabel.setCardinality(Cardinality.SINGLE);
        return this;
    }

    @Override
    public EdgeLabelMaker multiple() {
        this.edgeLabel.setCardinality(Cardinality.MULTIPLE);
        return this;
    }

    @Override
    public EdgeLabelMaker signature(String... keys) {
        if (partitionKeys == null) {
            partitionKeys = new ArrayList<>();
        }
        partitionKeys.addAll(Arrays.asList(keys));
        return this;
    }

    @Override
    public EdgeLabelMaker inOne2One() {
        this.edgeLabel.setMultiplicity(Multiplicity.ONE2ONE);
        return this;
    }

    @Override
    public EdgeLabelMaker inOne2Many() {
        this.edgeLabel.setMultiplicity(Multiplicity.ONE2MANY);
        return this;
    }

    @Override
    public EdgeLabelMaker inMany2Many() {
        this.edgeLabel.setMultiplicity(Multiplicity.MANY2MANY);
        return this;
    }

    @Override
    public EdgeLabelMaker inMany2One() {
        this.edgeLabel.setMultiplicity(Multiplicity.MANY2ONE);
        return this;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public SchemaType create() {
        // 如果Cardinality为MULTIPLE，但是没有设置分区键
        if (edgeLabel.cardinality() == Cardinality.MULTIPLE && !hasPartitionKeys()) {
            logger.error("The edgelabel with Cardinality.MULTIPLE must specified partition key.");
        }
        schemaStore.addEdgeLabel(edgeLabel);
        return edgeLabel;
    }

    private boolean hasPartitionKeys() {
        return partitionKeys != null && !partitionKeys.isEmpty();
    }

    @Override
    public SchemaType add() {
        return null;
    }

    @Override
    public void remove() {

    }
}
