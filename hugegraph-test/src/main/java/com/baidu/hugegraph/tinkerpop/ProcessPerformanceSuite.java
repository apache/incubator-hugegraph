///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
//package com.baidu.hugegraph.tinkerpop;
//
//import org.apache.commons.configuration.ConfigurationException;
//import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
//import org.apache.tinkerpop.gremlin.GraphManager;
//import org.apache.tinkerpop.gremlin.GraphProvider;
//import org.apache.tinkerpop.gremlin.process.TraversalPerformanceTest;
//import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
//import org.apache.tinkerpop.gremlin.structure.Graph;
//import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
//import org.junit.runners.model.InitializationError;
//import org.junit.runners.model.RunnerBuilder;
//import org.junit.runners.model.Statement;
//
//import com.baidu.hugegraph.dist.RegisterUtil;
//
///**
// * The {@code ProcessPerformanceSuite} is a JUnit test runner that executes the
// * Gremlin Test Suite over a Graph implementation.  This suite contains
// * "long-run" tests that produce reports on the traversal execution
// * performance of a vendor implementation {@link Graph}. Its usage is optional
// * to providers as the tests are somewhat redundant to those found elsewhere
// * in other required test suites.
// * For more information on the usage of this suite,
// * please see {@link StructureStandardSuite}.
// *
// * @author Stephen Mallette (http://stephen.genoprime.com)
// * @deprecated  As of release 3.2.0-incubating, replaced by gremlin-benchmark.
// */
//@Deprecated
//public class ProcessPerformanceSuite extends AbstractGremlinSuite {
//
//    /**
//     * This list of tests in the suite that will be executed.
//     * Gremlin developers should add to this list
//     * as needed to enforce tests upon implementations.
//     */
//    private static final Class<?>[] allTests = new Class<?>[]{
//            TraversalPerformanceTest.class
//    };
//
//    public ProcessPerformanceSuite(final Class<?> klass,
//                                   final RunnerBuilder builder)
//                                   throws InitializationError,
//                                          ConfigurationException {
//        super(klass, builder, allTests, null, true,
//              TraversalEngine.Type.STANDARD);
//        RegisterUtil.registerBackends();
//    }
//
//    @Override
//    protected Statement withAfterClasses(final Statement statement) {
//        Statement wrappedStatement = new Statement() {
//            @Override
//            public void evaluate() throws Throwable {
//                statement.evaluate();
//                GraphProvider gp = GraphManager.setGraphProvider(null);
//                ((TestGraphProvider) gp).clear();
//                GraphManager.setGraphProvider(gp);
//            }
//        };
//        return super.withAfterClasses(wrappedStatement);
//    }
//}
