package com.baidu.hugegraph.schema;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.type.define.EdgeLink;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.StringUtil;

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

    public void checkExists(boolean checkExists) {
        this.checkExits = checkExists;
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
            throw new HugeException(String.format("The edgeLabel: %s has "
                    + "exised.", this.name));
        }

        this.checkLinks();
        this.checkProperties();
        this.checkSortKeys();

        this.transaction().addEdgeLabel(this);
        return this;
    }

    @Override
    public void remove() {
        this.transaction().removeEdgeLabel(this.name);
    }

    @Override
    protected HugeEdgeLabel copy() throws CloneNotSupportedException {
        HugeEdgeLabel edgeLabel = new HugeEdgeLabel(this.name);
        edgeLabel.links = new LinkedHashSet<>();
        for (EdgeLink link : this.links) {
            edgeLabel.links.add(EdgeLink.of(link.source(), link.target()));
        }
        edgeLabel.frequency(this.frequency);
        edgeLabel.properties = new LinkedHashSet<>();
        for (String property : this.properties) {
            edgeLabel.properties.add(property);
        }
        edgeLabel.sortKeys = new LinkedHashSet<>();
        for (String primaryKey : this.sortKeys) {
            edgeLabel.sortKeys.add(primaryKey);
        }
        edgeLabel.indexNames = new LinkedHashSet<>();
        for (String indexName : this.indexNames) {
            edgeLabel.indexNames.add(indexName);
        }
        return edgeLabel;
    }

    private void checkLinks() {
        E.checkNotNull(this.links, HugeKeys.LINKS.string());
        E.checkNotEmpty(this.links, HugeKeys.LINKS.string());

        for (EdgeLink link : this.links) {
            VertexLabel src = this.transaction().getVertexLabel(link.source());
            E.checkArgumentNotNull(src, "Undefined vertex label: %s",
                    link.source());
            VertexLabel tgt = this.transaction().getVertexLabel(link.target());
            E.checkArgumentNotNull(tgt, "Undefined vertex label: %s",
                    link.target());
        }
    }

    private void checkProperties() {
        E.checkNotNull(this.properties, "The properties of %s", this);
        // The properties of edge label allowded be empty.
        // If properties is not empty, check all property.
        for (String pkName : this.properties) {
            PropertyKey propertyKey = this.transaction().getPropertyKey(pkName);
            E.checkArgumentNotNull(propertyKey, "Undefined property key: %s",
                    pkName);
        }
    }

    private void checkSortKeys() {
        if (this.frequency == Frequency.SINGLE) {
            E.checkArgument(this.sortKeys.isEmpty(),
                    "EdgeLabel can not contain sortKeys when the " +
                    "cardinality property is single.");
        } else {
            E.checkNotNull(this.sortKeys,
                    "The sortKeys can not be null when the cardinality " +
                    "property is multiple.");
            E.checkArgument(!this.sortKeys.isEmpty(),
                    "EdgeLabel must contain sortKeys when the cardinality " +
                    "property is multiple.");
        }

        if (this.sortKeys != null && !this.sortKeys.isEmpty()) {
            // Check whether the properties contains the specified keys
            E.checkArgument(!this.properties.isEmpty(),
                    "Properties can not be empty when exist sort keys.");
            for (String key : this.sortKeys) {
                E.checkArgument(this.properties.contains(key),
                        "Properties must contain the sort key: %s", key);
            }
        }
    }

}
