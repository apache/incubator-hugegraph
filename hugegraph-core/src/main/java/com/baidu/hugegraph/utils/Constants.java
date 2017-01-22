/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.utils;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * Created by zhangsuochao on 17/2/7.
 */
public class Constants {
    /**
     * Table names
     */
    public static final String EDGES = "edges";
    public static final String VERTICES = "vertices";

    /**
     * Default column family
     */
    public static final String DEFAULT_FAMILY = "cf";
    public static final byte[] DEFAULT_FAMILY_BYTES = Bytes.toBytes(DEFAULT_FAMILY);

    /**
     * Internal keys
     */
    public static final String LABEL = Graph.Hidden.hide("l");
    public static final String FROM = Graph.Hidden.hide("f");
    public static final String TO = Graph.Hidden.hide("t");
    public static final String CREATED_AT = Graph.Hidden.hide("c");
    public static final String UPDATED_AT = Graph.Hidden.hide("u");
    public static final String UNIQUE = Graph.Hidden.hide("q");
    public static final String ELEMENT_ID = Graph.Hidden.hide("i");
    public static final String EDGE_ID = Graph.Hidden.hide("e");
    public static final String VERTEX_ID = Graph.Hidden.hide("v");
    public static final String INDEX_STATE = Graph.Hidden.hide("x");

    public static final byte[] LABEL_BYTES = Bytes.toBytes(LABEL);
    public static final byte[] FROM_BYTES = Bytes.toBytes(FROM);
    public static final byte[] TO_BYTES = Bytes.toBytes(TO);
    public static final byte[] CREATED_AT_BYTES = Bytes.toBytes(CREATED_AT);
    public static final byte[] UPDATED_AT_BYTES = Bytes.toBytes(UPDATED_AT);
    public static final byte[] UNIQUE_BYTES = Bytes.toBytes(UNIQUE);
    public static final byte[] ELEMENT_ID_BYTES = Bytes.toBytes(ELEMENT_ID);
    public static final byte[] EDGE_ID_BYTES = Bytes.toBytes(EDGE_ID);
    public static final byte[] VERTEX_ID_BYTES = Bytes.toBytes(VERTEX_ID);
    public static final byte[] INDEX_STATE_BYTES = Bytes.toBytes(INDEX_STATE);

    /**
     * Unlimited time-to-live.
     */
    //  public static final int FOREVER = -1;
    public static final int FOREVER = Integer.MAX_VALUE;
}
