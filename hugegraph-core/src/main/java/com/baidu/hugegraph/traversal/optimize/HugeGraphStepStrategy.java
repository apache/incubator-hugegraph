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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public final class HugeGraphStepStrategy
             extends AbstractTraversalStrategy<ProviderOptimizationStrategy>
             implements ProviderOptimizationStrategy {

    private static final long serialVersionUID = -2952498905649139719L;

    private static final HugeGraphStepStrategy INSTANCE =
                                               new HugeGraphStepStrategy();

    private HugeGraphStepStrategy() {
        // pass
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void apply(Traversal.Admin<?, ?> traversal) {
        // Extract conditions in GraphStep
        List<GraphStep> steps = TraversalHelper.getStepsOfClass(
                                GraphStep.class, traversal);
        for (GraphStep originStep : steps) {
            HugeGraphStep<?, ?> newStep = new HugeGraphStep<>(originStep);
            TraversalHelper.replaceStep(originStep, newStep, traversal);
            extractHasContainer(newStep, traversal);
            extractRange(newStep, traversal);
        }
    }

    protected static void extractHasContainer(HugeGraphStep<?, ?> newStep,
                                              Traversal.Admin<?, ?> traversal) {
        Step<?, ?> step = newStep;
        do {
            step = step.getNextStep();
            if (step instanceof HasStep) {
                HasContainerHolder holder = (HasContainerHolder) step;
                for (HasContainer has : holder.getHasContainers()) {
                    if (!GraphStep.processHasContainerIds(newStep, has)) {
                        newStep.addHasContainer(has);
                    }
                }
                TraversalHelper.copyLabels(step, step.getPreviousStep(), false);
                traversal.removeStep(step);
            }
        } while (step instanceof HasStep || step instanceof NoOpBarrierStep);
    }

    protected static void extractRange(HugeGraphStep<?, ?> newStep,
                                       Traversal.Admin<?, ?> traversal) {
        Step<?, ?> step = newStep;
        do {
            step = step.getNextStep();
            if (step instanceof RangeGlobalStep) {
                // NOTE: we just deal with the first limit
                // (maybe should the min one)
                RangeGlobalStep<?> range = (RangeGlobalStep<?>) step;
                newStep.setRange(range.getLowRange(), range.getHighRange());
                // NOTE: keep the step to filter results after query from DB
                // due to DB may not be implemented correctly
                // traversal.removeStep(step);
                break;
            }
        } while (step instanceof NoOpBarrierStep);
    }

    public static HugeGraphStepStrategy instance() {
        return INSTANCE;
    }

}
