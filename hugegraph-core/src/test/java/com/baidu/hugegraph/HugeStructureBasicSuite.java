/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph;

import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.algorithm.generator.DistributionGeneratorTest;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.EdgeTest;
import org.apache.tinkerpop.gremlin.structure.FeatureSupportTest;
import org.apache.tinkerpop.gremlin.structure.io.IoCustomTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * Created by zhangsuochao on 17/2/20.
 */
public class HugeStructureBasicSuite extends AbstractGremlinSuite {
    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] allTests = new Class<?>[] {
            //            CommunityGeneratorTest.class,
            //            DetachedGraphTest.class,
            //            DetachedEdgeTest.class,
            //            DetachedVertexPropertyTest.class,
            //            DetachedPropertyTest.class,
            //            DetachedVertexTest.class,
            DistributionGeneratorTest.class,
            EdgeTest.class,
            FeatureSupportTest.class,
            IoCustomTest.class,
            //            IoEdgeTest.class,
            //            IoGraphTest.class,
            //            IoVertexTest.class,
            //            IoPropertyTest.class,
            //            GraphTest.class,
            //            GraphConstructionTest.class,
            //            IoTest.class,
            //            VertexPropertyTest.class,
            //            VariablesTest.class,
            //            PropertyTest.class,
            //            ReferenceGraphTest.class,
            //            ReferenceEdgeTest.class,
            //            ReferenceVertexPropertyTest.class,
            //            ReferenceVertexTest.class,
            //            SerializationTest.class,
            //            StarGraphTest.class,
            //            TransactionTest.class,
            //            VertexTest.class
    };

    @SuppressWarnings("deprecation")
    public HugeStructureBasicSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, allTests, null, false, TraversalEngine.Type.STANDARD);
    }
}
