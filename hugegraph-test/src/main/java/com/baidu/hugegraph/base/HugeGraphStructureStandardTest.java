/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.base;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.server.HugeGraphServer;
import com.baidu.hugegraph.server.RegisterUtil;

/**
 * Created by zhangsuochao on 17/5/3.
 */
@RunWith(HugeStructureBasicSuite.class)
@GraphProviderClass(provider = HugeGraphProvider.class, graph = HugeGraph.class)
public class HugeGraphStructureStandardTest {

}
