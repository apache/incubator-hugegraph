package com.baidu.hugegraph.schema;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.type.define.EdgeLink;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.util.StringUtil;
import com.google.common.base.Preconditions;

/**
 * Created by liningrui on 2017/3/20.
 */
public class HugeEdgeLabel extends EdgeLabel {

    private Frequency frequency;
    private Set<EdgeLink> links;
    private Set<String> sortKeys;

    public HugeEdgeLabel(String name) {
        super(name);
        this.frequency = Frequency.SINGLE;
        this.links = new LinkedHashSet<>();
        this.sortKeys = new LinkedHashSet<>();
    }

    @Override
    public HugeEdgeLabel indexNames(String... names) {
        this.indexNames.addAll(Arrays.asList(names));
        return this;
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
    public Set<EdgeLink> links() {
        return this.links;
    }

    @Override
    public EdgeLabel link(String src, String tgt) {
        EdgeLink pair = EdgeLink.of(src, tgt);
        this.links.add(pair);
        return this;
    }

    @Override
    public void links(Set<EdgeLink> links) {
        this.links = links;
    }

    public void links(EdgeLink... links) {
        this.links.addAll(Arrays.asList(links));
    }

    public boolean checkLink(String src, String tgt) {
        return this.links().contains(EdgeLink.of(src, tgt));
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
            for (EdgeLink link : this.links) {
                linkSchema += ".link(\"";
                linkSchema += link.source();
                linkSchema += "\",\"";
                linkSchema += link.target();
                linkSchema += "\")";
            }
        }
        return linkSchema;
    }

    @Override
    public String schema() {
        return "schema.edgeLabel(\"" + this.name + "\")"
                + "." + this.frequency.string() + "()"
                + "." + propertiesSchema()
                + linkSchema()
                + StringUtil.desc("sortKeys", this.sortKeys)
                + ".create();";
    }

    @Override
    public EdgeLabel create() {

        StringUtil.checkName(this.name);
        // Try to read
        EdgeLabel edgeLabel = this.transaction().getEdgeLabel(this.name);
        // if edgeLabel exist and checkExits
        if (edgeLabel != null && this.checkExits) {
            throw new HugeException("The edgeLabel:" + this.name + " has exised.");
        }

        this.checkSortKeys();
        this.transaction().addEdgeLabel(this);
        return this;
    }

    @Override
    public void remove() {
        this.transaction().removeEdgeLabel(this.name);
    }

    private void checkSortKeys() {
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
