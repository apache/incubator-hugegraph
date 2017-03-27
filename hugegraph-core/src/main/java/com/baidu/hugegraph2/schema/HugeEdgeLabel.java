package com.baidu.hugegraph2.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.javatuples.Pair;

import com.baidu.hugegraph2.backend.tx.SchemaTransaction;
import com.baidu.hugegraph2.type.define.Cardinality;
import com.baidu.hugegraph2.type.define.Multiplicity;
import com.baidu.hugegraph2.type.schema.EdgeLabel;
import com.baidu.hugegraph2.type.schema.SchemaElement;

/**
 * Created by liningrui on 2017/3/20.
 */
public class HugeEdgeLabel implements EdgeLabel {

    private String name;
    private SchemaTransaction transaction;
    // multiplicity：图的角度，描述多个顶点之间的关系。多对一，多对多，一对多，一对一
    private Multiplicity multiplicity;
    // cardinality：两个顶点之间是否可以有多条边
    private Cardinality cardinality;

    // 每组被连接的顶点形成一个Pair，一类边可能在多组顶点间建立连接
    private List<Pair<String, String>> links;

    private Set<String> partitionKeys;
    private Set<String> properties;


    public HugeEdgeLabel(String name, SchemaTransaction transaction) {
        this.name = name;
        this.transaction = transaction;
        this.multiplicity = Multiplicity.ONE2ONE;
        this.cardinality = Cardinality.SINGLE;
        this.links = null;
        this.partitionKeys = null;
        this.properties = null;
    }

    @Override
    public Multiplicity multiplicity() {
        return multiplicity;
    }

    public void multiplicity(Multiplicity multiplicity) {
        this.multiplicity = multiplicity;
    }

    @Override
    public Cardinality cardinality() {
        return cardinality;
    }

    public void cardinality(Cardinality cardinality) {
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
    public EdgeLabel linkOne2One() {
        this.multiplicity(Multiplicity.ONE2ONE);
        return this;
    }

    @Override
    public EdgeLabel linkOne2Many() {
        this.multiplicity(Multiplicity.ONE2MANY);
        return this;
    }

    @Override
    public EdgeLabel linkMany2Many() {
        this.multiplicity(Multiplicity.MANY2MANY);
        return this;
    }

    @Override
    public EdgeLabel linkMany2One() {
        this.multiplicity(Multiplicity.MANY2ONE);
        return this;
    }

    @Override
    public EdgeLabel single() {
        this.cardinality(Cardinality.SINGLE);
        return this;
    }

    @Override
    public EdgeLabel multiple() {
        this.cardinality(Cardinality.MULTIPLE);
        return this;
    }

    @Override
    public EdgeLabel link(String srcName, String tgtName) {
        if (links == null) {
            links = new ArrayList<>();
        }
        Pair<String, String> pair = new Pair<>(srcName, tgtName);
        links.add(pair);
        return this;
    }

    @Override
    public String schema() {
        return null;
    }

    @Override
    public String name() {
        return name;
    }


    @Override
    public void partitionKeys(String... keys) {
        if (partitionKeys == null) {
            partitionKeys = new HashSet<>();
        }
        partitionKeys.addAll(Arrays.asList(keys));
    }

    @Override
    public Set<String> properties() {
        return properties;
    }

    @Override
    public SchemaElement properties(String... propertyNames) {
        if (properties == null) {
            properties = new HashSet<>();
        }
        properties.addAll(Arrays.asList(propertyNames));
        return this;
    }

    @Override
    public void create() {
        transaction.addEdgeLabel(this);
    }

    @Override
    public void remove() {
        transaction.removeEdgeLabel(name);
    }

    @Override
    public String toString() {
        return String.format("{name=%s, multiplicity=%s, cardinality=%s}",
                name, multiplicity.toString(), cardinality.toString());
    }
}
