/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.traversal.optimize;

import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public final class HugeVertexStepStrategy
             extends AbstractTraversalStrategy<ProviderOptimizationStrategy>
             implements ProviderOptimizationStrategy {

    private static final long serialVersionUID = 491355700217483162L;

    private static final HugeVertexStepStrategy INSTANCE;

    static {
        INSTANCE = new HugeVertexStepStrategy();
    }

    private HugeVertexStepStrategy() {
        // pass
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void apply(final Traversal.Admin<?, ?> traversal) {
        TraversalUtil.convAllHasSteps(traversal);

        List<VertexStep> steps = TraversalHelper.getStepsOfClass(
                                 VertexStep.class, traversal);
        for (VertexStep originStep : steps) {
            HugeVertexStep<?> newStep = new HugeVertexStep<>(originStep);
            TraversalHelper.replaceStep(originStep, newStep, traversal);

            TraversalUtil.extractHasContainer(newStep, traversal);

            // TODO: support order-by optimize
            // TraversalUtil.extractOrder(newStep, traversal);

            TraversalUtil.extractRange(newStep, traversal, true);

            TraversalUtil.extractCount(newStep, traversal);
        }
    }

    public static HugeVertexStepStrategy instance() {
        return INSTANCE;
    }
}
