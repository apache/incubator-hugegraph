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

    private static final HugeGraphStepStrategy INSTANCE = new HugeGraphStepStrategy();

    private HugeGraphStepStrategy() {
        // pass
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void apply(Traversal.Admin<?, ?> traversal) {
        // extract conditions in GraphStep
        List<GraphStep> steps = TraversalHelper.getStepsOfClass(
                GraphStep.class, traversal);
        for (GraphStep originalStep : steps) {
            HugeGraphStep<?, ?> newStep = new HugeGraphStep<>(originalStep);
            TraversalHelper.replaceStep(originalStep, newStep, traversal);
            extractHasContainer(newStep, traversal);
            extractRange(newStep, traversal);
        }
    }

    protected static void extractHasContainer(HugeGraphStep<?, ?> newStep,
            Traversal.Admin<?, ?> traversal) {
        Step<?, ?> step = null;
        do {
            step = newStep.getNextStep();
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
        Step<?, ?> step = null;
        do {
            step = newStep.getNextStep();
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
