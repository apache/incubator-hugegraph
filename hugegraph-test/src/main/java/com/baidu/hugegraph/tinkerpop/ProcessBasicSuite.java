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

package com.baidu.hugegraph.tinkerpop;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.CoreTraversalTest;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComplexTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.OptionalTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.CoinTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.CyclicPathTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SimplePathTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CoalesceTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ConstantTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LoopsTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapKeysTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapValuesTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MinTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProjectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.UnfoldTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ExplainTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTestV3d0;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StoreTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.TranslationStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategyProcessTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import com.baidu.hugegraph.dist.RegisterUtil;

/**
 * Standard process test suite for tinkerpop graph
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ProcessBasicSuite extends AbstractGremlinSuite {
    /**
     * This list of tests in the suite that will be executed
     * as part of this suite.
     */
    private static final Class<?>[] allTests = new Class<?>[] {
            // 605 test, 137 failed, 33 ignore
            // branch
            BranchTest.Traversals.class,
            ChooseTest.Traversals.class, // 0+1+0
            OptionalTest.Traversals.class,
            LocalTest.Traversals.class, // 0+5+1
            RepeatTest.Traversals.class, // 0+1+0
            UnionTest.Traversals.class,

            // filter
            AndTest.Traversals.class,
            CoinTest.Traversals.class,
            CyclicPathTest.Traversals.class,
            DedupTest.Traversals.class, // 0+10+0
            DropTest.Traversals.class, // 0+0+1
            FilterTest.Traversals.class,
            HasTest.Traversals.class, // 0+2+4
            IsTest.Traversals.class,
            OrTest.Traversals.class,
            RangeTest.Traversals.class, // 0+1+0
            SampleTest.Traversals.class, // 0+2+0
            SimplePathTest.Traversals.class, // 0+1+0
            TailTest.Traversals.class, // 0+1+0
            WhereTest.Traversals.class, // 0+3+0

            // map
            AddEdgeTest.Traversals.class, // 9+0+1
            AddVertexTest.Traversals.class, // 7+4+2
            CoalesceTest.Traversals.class, // 2+0+0
            ConstantTest.Traversals.class,
            CountTest.Traversals.class, // 0+3+0
            FlatMapTest.Traversals.class,
            FoldTest.Traversals.class,
            GraphTest.Traversals.class, // 1+0+0
            LoopsTest.Traversals.class, // 0+3+0
            MapTest.Traversals.class,
            MapKeysTest.Traversals.class,
            MapValuesTest.Traversals.class,
            MatchTest.CountMatchTraversals.class, // 0+3+0
            MatchTest.GreedyMatchTraversals.class, // 0+3+0
            MaxTest.Traversals.class, // 0+2+0
            MeanTest.Traversals.class, // 0+1+0
            MinTest.Traversals.class, // 0+2+0
            SumTest.Traversals.class, // 0+1+0
            OrderTest.Traversals.class, // 0+4+1
            PathTest.Traversals.class, // 0+1+0
            ProfileTest.Traversals.class, // 2+8+1
            ProjectTest.Traversals.class,
            PropertiesTest.Traversals.class, // 0+3+0
            SelectTest.Traversals.class, // 0+1+1
            VertexTest.Traversals.class, // 3+5+0
            UnfoldTest.Traversals.class, // 0+2+0
            ValueMapTest.Traversals.class, // 0+2+0

            // sideEffect
            AggregateTest.Traversals.class,
            ExplainTest.Traversals.class,
            GroupTest.Traversals.class, // (assertError)7
            GroupTestV3d0.Traversals.class, // (assertError)2
            GroupCountTest.Traversals.class, // (assertError)3
            InjectTest.Traversals.class,
            SackTest.Traversals.class, // (assertError)2
            SideEffectCapTest.Traversals.class,
            SideEffectTest.Traversals.class,
            StoreTest.Traversals.class, // (assertError)1
            SubgraphTest.Traversals.class, // (ignore)3
            TreeTest.Traversals.class, // (assertError)1

            // compliance
            ComplexTest.Traversals.class,
            CoreTraversalTest.class, // (exception)4+0+1(ignore)
            TraversalInterruptionTest.class, // (assertError)4

            // creations
            TranslationStrategyProcessTest.class,

            // decorations
            ElementIdStrategyProcessTest.class,
            EventStrategyProcessTest.class, // (exceptions)1+3+4
            ReadOnlyStrategyProcessTest.class,
            PartitionStrategyProcessTest.class, // (exceptions)6+0+12
            SubgraphStrategyProcessTest.class // (assertError)
    };

    /**
     * A list of the minimum set of base tests that
     * Gremlin flavors should implement to be compliant with Gremlin.
     */
    private static final Class<?>[] testsToEnforce = new Class<?>[] {
            // branch
            BranchTest.class,
            ChooseTest.class,
            OptionalTest.class,
            LocalTest.class,
            RepeatTest.class,
            UnionTest.class,

            // filter
            AndTest.class,
            CoinTest.class,
            CyclicPathTest.class,
            DedupTest.class,
            DropTest.class,
            FilterTest.class,
            HasTest.class,
            IsTest.class,
            OrTest.class,
            RangeTest.class,
            SampleTest.class,
            SimplePathTest.class,
            TailTest.class,
            WhereTest.class,

            // map
            AddEdgeTest.class,
            AddVertexTest.class,
            CoalesceTest.class,
            ConstantTest.class,
            CountTest.class,
            FlatMapTest.class,
            FoldTest.class,
            LoopsTest.class,
            MapTest.class,
            MapKeysTest.class,
            MapValuesTest.class,
            MatchTest.class,
            MaxTest.class,
            MeanTest.class,
            MinTest.class,
            SumTest.class,
            OrderTest.class,
            PathTest.class,
            PropertiesTest.class,
            ProfileTest.class,
            ProjectTest.class,
            SelectTest.class,
            VertexTest.class,
            UnfoldTest.class,
            ValueMapTest.class,

            // sideEffect
            AggregateTest.class,
            GroupTest.class,
            GroupCountTest.class,
            InjectTest.class,
            SackTest.class,
            SideEffectCapTest.class,
            SideEffectTest.class,
            StoreTest.class,
            SubgraphTest.class,
            TreeTest.class,
    };

    /**
     * This constructor is used by JUnit and will run this suite with
     * its concrete implementations of the {@code testsToEnforce}.
     */
    public ProcessBasicSuite(final Class<?> klass,
                             final RunnerBuilder builder)
                             throws InitializationError,
                                    ConfigurationException {
        super(klass, builder, allTests, testsToEnforce, true,
              TraversalEngine.Type.STANDARD);
        RegisterUtil.registerBackends();
    }

    /**
     * This constructor is used by Gremlin flavor implementers
     * who supply their own implementations of the {@code testsToEnforce}.
     */
    public ProcessBasicSuite(final Class<?> klass,
                             final RunnerBuilder builder,
                             final Class<?>[] testsToExecute)
                             throws InitializationError,
                                    ConfigurationException {
        super(klass, builder, testsToExecute, testsToEnforce, true,
              TraversalEngine.Type.STANDARD);
        RegisterUtil.registerBackends();
    }
}
