/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.base;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import com.baidu.hugegraph.dist.RegisterUtil;

/**
 * Standard test suite for tinkerpop graph
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class HugeStructureBasicSuite extends AbstractGremlinSuite {
    /**
     * This list of tests in the suite that will be executed.
     * Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] allTests = new Class<?>[] {
            CommunityGeneratorTest.class,
            //            DetachedGraphTest.class,
            //            DetachedEdgeTest.class,
            //            DetachedVertexPropertyTest.class,
            //            DetachedPropertyTest.class,
            //            DetachedVertexTest.class,
            //            DistributionGeneratorTest.class,
            //            EdgeTest.class,
            //            FeatureSupportTest.class,
            //            IoCustomTest.class,
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
            //            ReferenceVertexTestVertexTest.class
    };

    @SuppressWarnings("deprecation")
    public HugeStructureBasicSuite(final Class<?> klass, final RunnerBuilder builder)
            throws InitializationError, ConfigurationException {
        super(klass, builder, allTests, null, false,
                TraversalEngine.Type.STANDARD);

        String confFile = "conf/hugegraph-test.yaml";
        RegisterUtil.registerBackends(confFile);

    }
}
