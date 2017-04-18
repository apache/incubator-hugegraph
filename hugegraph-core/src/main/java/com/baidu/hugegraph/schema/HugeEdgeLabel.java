package com.baidu.hugegraph.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.type.define.Multiplicity;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.util.StringUtil;
import com.google.common.base.Preconditions;

/**
 * Created by liningrui on 2017/3/20.
 */
public class HugeEdgeLabel extends EdgeLabel {

    private static final Logger logger = LoggerFactory.getLogger(HugeEdgeLabel.class);

    private Multiplicity multiplicity;
    private Frequency frequency;
    private List<Pair<String, String>> links;
    private Set<String> sortKeys;

    public HugeEdgeLabel(String name, SchemaTransaction transaction) {
        super(name, transaction);
        this.multiplicity = Multiplicity.ONE2ONE;
        this.frequency = Frequency.SINGLE;
        this.links = new ArrayList<>();
        this.sortKeys = new LinkedHashSet<>();
    }

    @Override
    public Multiplicity multiplicity() {
        return this.multiplicity;
    }

    public void multiplicity(Multiplicity multiplicity) {
        this.multiplicity = multiplicity;
    }

    @Override
    public Frequency frequency() {
        return this.frequency;
    }

    public void frequency(Frequency frequency) {
        this.frequency = frequency;
    }

    @Override
    public boolean isDirected() {
        // TODO: please implement
        return true;
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
    public EdgeLabel singleTime() {
        this.frequency(Frequency.SINGLE);
        return this;
    }

    @Override
    public EdgeLabel multiTimes() {
        this.frequency(Frequency.MULTIPLE);
        return this;
    }

    public List<Pair<String, String>> links() {
        return links;
    }

    @Override
    public EdgeLabel link(String src, String tgt) {
        Pair<String, String> pair = new Pair<>(src, tgt);
        this.links.add(pair);
        return this;
    }

    @Override
    public Set<String> sortKeys() {
        return this.sortKeys;
    }

    @Override
    public EdgeLabel sortKeys(String... keys) {
        this.sortKeys.addAll(Arrays.asList(keys));
        return this;
    }

    @Override
    public String toString() {
        return String.format("{name=%s, multiplicity=%s, cardinality=%s}",
                this.name, this.multiplicity.toString(), this.frequency.toString());
    }

    public String linkSchema() {
        String linkSchema = "";
        if (this.links != null) {
            for (Pair<String, String> link : this.links) {
                linkSchema += ".link(\"";
                linkSchema += link.getValue0();
                linkSchema += "\",\"";
                linkSchema += link.getValue1();
                linkSchema += "\")";
            }
        }
        return linkSchema;
    }

    @Override
    public String schema() {
        this.schema = "schema.edgeLabel(\"" + this.name + "\")"
                + "." + this.frequency.schema() + "()"
                + "." + this.multiplicity.schema() + "()"
                + "." + propertiesSchema()
                + linkSchema()
                + StringUtil.descSchema("sortKeys", this.sortKeys)
                + ".create();";
        return this.schema;
    }

    @Override
    public void create() {
        if (this.transaction.getEdgeLabel(this.name) != null) {
            throw new HugeException("The edgeLabel:" + this.name + " has exised.");
        }

        StringUtil.verifyName(name);
        verifySortKeys();

        this.transaction.addEdgeLabel(this);
        this.commit();
    }

    @Override
    public void remove() {
        this.transaction.removeEdgeLabel(this.name);
    }

    private void verifySortKeys() {
        if (this.frequency == Frequency.SINGLE) {
            Preconditions.checkArgument(sortKeys.isEmpty(), "edgeLabel can not contain sortKeys when the cardinality"
                    + " property is single.");
        } else {
            Preconditions.checkNotNull(sortKeys, "the sortKeys can not be null when the cardinality property is "
                    + "multiple.");
            Preconditions.checkArgument(!sortKeys.isEmpty(), "edgeLabel must contain sortKeys when the cardinality"
                    + " property is multiple.");
        }

        if (sortKeys != null && !sortKeys.isEmpty()) {
            // Check whether the properties contains the specified keys
            Preconditions.checkNotNull(properties, "properties can not be null");
            Preconditions.checkArgument(!properties.isEmpty(), "properties can not be empty");
            for (String key : sortKeys) {
                Preconditions.checkArgument(properties.containsKey(key),
                        "Properties must contain the specified key : " + key);
            }
        }
    }

}
