/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.structure;

import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;

/**
 * Created by zhangsuochao on 17/2/6.
 */
public class HugeProperty<V> implements Property<V> {
    protected final Graph graph;
    protected final HugeElement element;
    protected final String key;
    protected final V value;

    public HugeProperty(final Graph graph, final HugeElement element, final String key, final V value) {
        this.graph = graph;
        this.element = element;
        this.key = key;
        this.value = value;
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return null != this.value;
    }

    @Override
    public Element element() {
        return this.element;
    }

    @Override
    public void remove() {

    }
}
