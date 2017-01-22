/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.structure;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

import com.baidu.hugegraph.HugeGraphProvider;
import com.baidu.hugegraph.HugeStructureBasicSuite;

/**
 * Created by zhangsuochao on 17/2/13.
 */
@RunWith(HugeStructureBasicSuite.class)
@GraphProviderClass(provider = HugeGraphProvider.class, graph = HugeGraph.class)
public class HugeGraphStructureStandardTest {
}
