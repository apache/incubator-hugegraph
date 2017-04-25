package com.baidu.hugegraph.traversal.optimize;

import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public final class HugeVertexStepStrategy
        extends AbstractTraversalStrategy<ProviderOptimizationStrategy>
        implements ProviderOptimizationStrategy {

    private static final long serialVersionUID = 491355700217483162L;

    private static final HugeVertexStepStrategy INSTANCE = new HugeVertexStepStrategy();

    private HugeVertexStepStrategy() {
        // pass
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        List<VertexStep> steps = TraversalHelper.getStepsOfClass(
                VertexStep.class, traversal);
        for (VertexStep originalStep : steps) {
            HugeVertexStep<?> newStep = new HugeVertexStep<>(originalStep);
            TraversalHelper.replaceStep(originalStep, newStep, traversal);
            extractHasContainer(newStep, traversal);
        }
    }

    protected static void extractHasContainer(HugeVertexStep<?> newStep,
            Traversal.Admin<?, ?> traversal) {
        Step<?, ?> step = null;
        do {
            step = newStep.getNextStep();
            if (step instanceof HasStep) {
                HasContainerHolder holder = (HasContainerHolder) step;
                for (HasContainer has : holder.getHasContainers()) {
                    newStep.addHasContainer(has);
                }
                TraversalHelper.copyLabels(step, step.getPreviousStep(), false);
                traversal.removeStep(step);
            }
        } while (step instanceof HasStep || step instanceof NoOpBarrierStep);
    }

    public static HugeVertexStepStrategy instance() {
        return INSTANCE;
    }

}
