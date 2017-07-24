package com.baidu.hugegraph.schema;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.exception.ExistedException;
import com.baidu.hugegraph.exception.NotAllowException;
import com.baidu.hugegraph.type.define.EdgeLink;
import com.baidu.hugegraph.type.define.Frequency;
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
    private List<String> sortKeys;

    public HugeEdgeLabel(String name) {
        super(name);
        this.frequency = Frequency.SINGLE;
        this.links = new LinkedHashSet<>();
        this.sortKeys = new LinkedList<>();
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
    public List<String> sortKeys() {
        return this.sortKeys;
    }

    @Override
    public EdgeLabel sortKeys(String... keys) {
        for (String key : keys) {
            if (!this.sortKeys.contains(key)) {
                this.sortKeys.add(key);
            }
        }
        return this;
    }

    public void checkExist(boolean checkExists) {
        this.checkExist = checkExists;
    }

    public String linkSchema() {
        StringBuilder sb = new StringBuilder();
        for (EdgeLink link : this.links) {
            sb.append(".link(\"");
            sb.append(link.source);
            sb.append("\",\"");
            sb.append(link.target);
            sb.append("\")");
        }
        return sb.toString();
    }

    @Override
    public String schema() {
        StringBuilder sb = new StringBuilder();
        sb.append("schema.makeEdgeLabel(\"").append(this.name).append("\")");
        sb.append(this.propertiesSchema());
        sb.append(this.linkSchema());
        sb.append(this.frequency.schema());
        sb.append(this.sortKeysSchema());
        sb.append(".ifNotExist()");
        sb.append(".create();");
        return sb.toString();
    }

    private String sortKeysSchema() {
        return StringUtil.desc("sortKeys", this.sortKeys);
    }

    @Override
    public EdgeLabel create() {

        StringUtil.checkName(this.name);
        // Try to read
        EdgeLabel edgeLabel = this.transaction().getEdgeLabel(this.name);
        // If edgeLabel exist and checkExist
        if (edgeLabel != null) {
            if (this.checkExist) {
                throw new ExistedException("edge label", this.name);
            } else {
                return edgeLabel;
            }
        }

        this.checkLinks();
        this.checkProperties();
        this.checkSortKeys();

        this.transaction().addEdgeLabel(this);
        return this;
    }

    @Override
    public EdgeLabel append() {

        StringUtil.checkName(this.name);
        // Don't allow user to modify some stable properties.
        this.checkStableVars();

        this.checkLinks();
        this.checkProperties();

        // Try to read
        EdgeLabel edgeLabel = this.transaction().getEdgeLabel(this.name);
        if (edgeLabel == null) {
            throw new HugeException(
                      "Can't append the edge label '%s' since it doesn't exist",
                      this.name);
        }

        this.checkFrequency(edgeLabel.frequency());

        edgeLabel.links().addAll(this.links);
        edgeLabel.properties().addAll(this.properties);

        this.transaction().addEdgeLabel(edgeLabel);
        return this;
    }

    @Override
    public EdgeLabel eliminate() {
        throw new HugeException("Not support eliminate action on edge label");
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
        edgeLabel.sortKeys = new LinkedList<>();
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
        E.checkNotNull(this.links, "links");
        E.checkNotEmpty(this.links, "links");

        for (EdgeLink link : this.links) {
            VertexLabel src = this.transaction().getVertexLabel(link.source());
            E.checkArgumentNotNull(src, "The vertex label '%s' is undefined " +
                                   "in the links of edge label '%s'",
                                   link.source(),
                                   this.name);
            VertexLabel tgt = this.transaction().getVertexLabel(link.target());
            E.checkArgumentNotNull(tgt, "The vertex label '%s' is undefined " +
                                   "in the links of edge label '%s'",
                                   link.target(),
                                   this.name);
        }
    }

    private void checkProperties() {
        E.checkNotNull(this.properties, "properties", this.name);
        // The properties of edge label allowded be empty.
        // If properties is not empty, check all property.
        for (String pk : this.properties) {
            PropertyKey propertyKey = this.transaction().getPropertyKey(pk);
            E.checkArgumentNotNull(propertyKey,
                                   "The property key '%s' is undefined in " +
                                   "the edge label '%s'", pk, this.name);
        }
    }

    private void checkSortKeys() {
        if (this.frequency == Frequency.SINGLE) {
            E.checkArgument(this.sortKeys.isEmpty(),
                            "EdgeLabel can't contain sortKeys when the " +
                            "cardinality property is single");
        } else {
            E.checkState(this.sortKeys != null,
                         "The sortKeys can't be null when the " +
                         "cardinality property is multiple");
            E.checkArgument(!this.sortKeys.isEmpty(),
                            "EdgeLabel must contain sortKeys when the " +
                            "cardinality property is multiple");
        }

        if (this.sortKeys != null && !this.sortKeys.isEmpty()) {
            // Check whether the properties contains the specified keys
            E.checkArgument(!this.properties.isEmpty(),
                            "Properties can't be empty when exist sort keys");
            for (String key : this.sortKeys) {
                E.checkArgument(this.properties.contains(key),
                                "The sort key '%s' of edge label '%s' must " +
                                "be contained in %s", key, this.name,
                                this.properties);
            }
        }
    }

    private void checkFrequency(Frequency frequency) {
        // Don't allow to modify frequency.
        if (this.frequency != frequency) {
            throw new NotAllowException("Not allowed to modify frequency for " +
                                        "existed edge label '%s'", this.name);
        }
    }

    private void checkStableVars() {
        // Don't allow to append sort keys.
        if (!this.sortKeys.isEmpty()) {
            throw new NotAllowException("Not allowed to append sort keys for " +
                                        "existed edge label '%s'", this.name);
        }
        if (!this.indexNames.isEmpty()) {
            throw new NotAllowException("Not allowed to append indexes for " +
                                        "existed edge label '%s'", this.name);
        }
    }

    public void rebuildIndex() {
        this.transaction().rebuildIndex(this);
    }
}
