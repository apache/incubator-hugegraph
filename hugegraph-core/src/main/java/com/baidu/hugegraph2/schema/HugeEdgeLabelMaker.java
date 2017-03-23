package com.baidu.hugegraph2.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.backend.tx.SchemaTransaction;
import com.baidu.hugegraph2.schema.maker.EdgeLabelMaker;
import com.baidu.hugegraph2.type.define.Cardinality;
import com.baidu.hugegraph2.type.define.Multiplicity;
import com.baidu.hugegraph2.type.schema.SchemaType;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugeEdgeLabelMaker implements EdgeLabelMaker {

    private static final Logger logger = LoggerFactory.getLogger(HugeEdgeLabelMaker.class);

    private SchemaTransaction transaction;
    private HugeEdgeLabel edgeLabel;
    private String name;


    public HugeEdgeLabelMaker(SchemaTransaction transaction, String name) {
        this.name = name;
        this.transaction = transaction;
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
    public EdgeLabelMaker partitionKeys(String... keys) {
        this.edgeLabel.partitionKeys(keys);
        return this;
    }

    @Override
    public EdgeLabelMaker isOne2One() {
        this.edgeLabel.setMultiplicity(Multiplicity.ONE2ONE);
        return this;
    }

    @Override
    public EdgeLabelMaker isOne2Many() {
        this.edgeLabel.setMultiplicity(Multiplicity.ONE2MANY);
        return this;
    }

    @Override
    public EdgeLabelMaker isMany2Many() {
        this.edgeLabel.setMultiplicity(Multiplicity.MANY2MANY);
        return this;
    }

    @Override
    public EdgeLabelMaker isMany2One() {
        this.edgeLabel.setMultiplicity(Multiplicity.MANY2ONE);
        return this;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public SchemaType create() {
        // 如果Cardinality为MULTIPLE，但是没有设置分区键
        if (edgeLabel.cardinality() == Cardinality.MULTIPLE && !edgeLabel.hasPartitionKeys()) {
            logger.error("The edgelabel with Cardinality.MULTIPLE must specified partition key.");
            System.exit(-1);
        }
        transaction.addEdgeLabel(edgeLabel);
        return edgeLabel;
    }

    @Override
    public SchemaType add() {
        return null;
    }

    @Override
    public void remove() {

    }
}
