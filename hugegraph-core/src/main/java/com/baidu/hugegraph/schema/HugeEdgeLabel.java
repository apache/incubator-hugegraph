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

    public HugeEdgeLabel(String name) {
        super(name);
        this.multiplicity = Multiplicity.ONE2ONE;
        this.frequency = Frequency.SINGLE;
        this.links = new ArrayList<>();
        this.sortKeys = new LinkedHashSet<>();
    }

    @Override
    public HugeEdgeLabel indexNames(String... names) {
        this.indexNames.addAll(Arrays.asList(names));
        return this;
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
        // TODO: implement (do we need this method?)
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

    @Override
    public List<Pair<String, String>> links() {
        return this.links;
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
        return "schema.edgeLabel(\"" + this.name + "\")"
                + "." + this.frequency.string() + "()"
                + "." + this.multiplicity.string() + "()"
                + "." + propertiesSchema()
                + linkSchema()
                + StringUtil.descSchema("sortKeys", this.sortKeys)
                + ".create();";
    }

    @Override
    public EdgeLabel create() {

        StringUtil.verifyName(this.name);
        // Try to read
        EdgeLabel edgeLabel = this.transaction().getEdgeLabel(this.name);
        // if edgeLabel exist and checkExits
        if (edgeLabel != null && checkExits) {
            throw new HugeException("The edgeLabel:" + this.name + " has exised.");
        }

        verifySortKeys();
        this.transaction().addEdgeLabel(this);
        return this;
    }

    @Override
    public void remove() {
        this.transaction().removeEdgeLabel(this.name);
    }

    private void verifySortKeys() {
        if (this.frequency == Frequency.SINGLE) {
            Preconditions
                    .checkArgument(this.sortKeys.isEmpty(), "edgeLabel can not contain sortKeys when the cardinality"
                            + " property is single.");
        } else {
            Preconditions.checkNotNull(this.sortKeys, "the sortKeys can not be null when the cardinality property is "
                    + "multiple.");
            Preconditions.checkArgument(!this.sortKeys.isEmpty(), "edgeLabel must contain sortKeys when the cardinality"
                    + " property is multiple.");
        }

        if (this.sortKeys != null && !this.sortKeys.isEmpty()) {
            // Check whether the properties contains the specified keys
            Preconditions.checkNotNull(this.properties, "properties can not be null");
            Preconditions.checkArgument(!this.properties.isEmpty(), "properties can not be empty");
            for (String key : this.sortKeys) {
                Preconditions.checkArgument(this.properties.containsKey(key),
                        "Properties must contain the specified key : " + key);
            }
        }
    }

}
