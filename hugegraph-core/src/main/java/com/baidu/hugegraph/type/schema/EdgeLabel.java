package com.baidu.hugegraph.type.schema;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.baidu.hugegraph.schema.HugePropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.type.define.EdgeLink;

/**
 * Created by jishilei on 17/3/18.
 */
public abstract class EdgeLabel extends SchemaElement {

    public EdgeLabel(String name) {
        super(name);
    }

    @Override
    public HugeType type() {
        return HugeType.EDGE_LABEL;
    }

    @Override
    public EdgeLabel properties(String... propertyNames) {
        this.properties.addAll(Arrays.asList(propertyNames));
        return this;
    }

    public abstract boolean isDirected();

    public abstract Frequency frequency();

    public abstract EdgeLabel singleTime();

    public abstract EdgeLabel multiTimes();

    public abstract EdgeLabel link(String src, String tgt);

    public abstract Set<EdgeLink> links();

    public abstract void links(Set<EdgeLink> links);

    public abstract EdgeLabel sortKeys(String... keys);

    public abstract List<String> sortKeys();

    @Override
    public EdgeLabel ifNotExist() {
        this.checkExits = false;
        return this;
    }

    @Override
    public abstract EdgeLabel create();

}
