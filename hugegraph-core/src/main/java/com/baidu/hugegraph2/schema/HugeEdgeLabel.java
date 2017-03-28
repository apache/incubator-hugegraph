package com.baidu.hugegraph2.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.HugeException;
import com.baidu.hugegraph2.backend.tx.SchemaTransaction;
import com.baidu.hugegraph2.type.define.Cardinality;
import com.baidu.hugegraph2.type.define.Multiplicity;
import com.baidu.hugegraph2.type.schema.EdgeLabel;
import com.baidu.hugegraph2.util.StringUtil;
import com.google.common.base.Preconditions;

/**
 * Created by liningrui on 2017/3/20.
 */
public class HugeEdgeLabel extends EdgeLabel {

    private static final Logger logger = LoggerFactory.getLogger(HugeEdgeLabel.class);

    private Multiplicity multiplicity;
    private Cardinality cardinality;
    private List<Pair<String, String>> links;
    private Set<String> sortKeys;

    public HugeEdgeLabel(String name, SchemaTransaction transaction) {
        super(name, transaction);
        this.multiplicity = Multiplicity.ONE2ONE;
        this.cardinality = Cardinality.SINGLE;
        this.links = null;
        this.sortKeys = null;
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
    public EdgeLabel link(String src, String tgt) {
        if (links == null) {
            links = new ArrayList<>();
        }
        Pair<String, String> pair = new Pair<>(src, tgt);
        links.add(pair);
        return this;
    }

    public Set<String> sortKeys() {
        return sortKeys;
    }

    @Override
    public EdgeLabel sortKeys(String... keys) {
        // Check whether the properties contains the specified keys
        Preconditions.checkNotNull(properties);
        for (String key : keys) {
            Preconditions
                    .checkArgument(properties.containsKey(key),
                            "Properties must contain the specified key : " + key);
        }
        if (this.sortKeys == null) {
            this.sortKeys = new HashSet<>();
        }
        this.sortKeys.addAll(Arrays.asList(keys));
        return this;
    }

    @Override
    public String toString() {
        return String.format("{name=%s, multiplicity=%s, cardinality=%s}",
                name, multiplicity.toString(), cardinality.toString());
    }

    public String linkSchema() {
        String linkSchema = "";
        if (links != null) {
            for (Pair<String, String> link : links) {
                linkSchema += ".link(\"";
                linkSchema += link.getValue0();
                linkSchema += "\",\"";
                linkSchema += link.getValue1();
                linkSchema += "\")";
            }
        }
        return linkSchema;
    }

    public String schema() {
        schema = "schema.edgeLabel(\"" + name + "\")"
                + "." + cardinality.schema() + "()"
                + "." + multiplicity.schema() + "()"
                + "." + propertiesSchema()
                + linkSchema()
                + StringUtil.descSchema("sortKeys", sortKeys)
                + ".create();";
        return schema;
    }

    public void create() {
        if (this.transaction.getEdgeLabel(this.name) != null) {
            throw new HugeException("The edgeLabel:" + this.name + " has exised.");
        }
        this.transaction.addEdgeLabel(this);
        this.commit();
    }

    public void remove() {
        transaction.removeEdgeLabel(name);
    }
}
