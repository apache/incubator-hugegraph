package com.baidu.hugegraph.election;

import java.util.Optional;
import java.util.concurrent.locks.LockSupport;

import com.baidu.hugegraph.util.E;

public class RoleElectionStateMachineImpl implements RoleElectionStateMachine {

    private volatile boolean shutdown = false;
    private Config config;

    private volatile RoleState state;

    private final MetaDataAdapter metaDataAdapter;

    public RoleElectionStateMachineImpl(Config config, MetaDataAdapter adapter) {
        this.config = config;
        this.metaDataAdapter = adapter;
        this.state = new UnKnownState(null);
    }

    @Override
    public void shutdown() {
        this.shutdown = true;
    }

    @Override
    public void apply(StateMachineCallback stateMachineCallback) {
        int failCount = 0;
        StateMachineContextImpl context = new StateMachineContextImpl(this);
        while (!this.shutdown) {
            E.checkArgumentNotNull(this.state, "State don't be null");
            try {
                this.state = state.transform(context);
                Callback runnable = this.state.callback(stateMachineCallback);
                runnable.call(context);
                failCount = 0;
            } catch (Throwable e) {
                stateMachineCallback.error(context, e);
                failCount ++;
                if (failCount >= this.config.exceedsFailCount()) {
                    this.state = new SafeState(context.epoch());
                    Callback runnable = this.state.callback(stateMachineCallback);
                    runnable.call(context);
                }
            }
        }
    }

    private interface RoleState {

        RoleState transform(StateMachineContext context);

        Callback callback(StateMachineCallback callback);

        static void heartBeatPark(StateMachineContext context) {
            long heartBeatIntervalSecond = context.config().heartBeatIntervalSecond();
            LockSupport.parkNanos(heartBeatIntervalSecond * 1_000_000_000);
        }

        static void randomPark(StateMachineContext context) {
            long randomTimeout = context.config().randomTimeoutMillisecond();
            LockSupport.parkNanos(randomTimeout * 1_000_000);
        }
    }

    @FunctionalInterface
    private interface Callback {

        void call(StateMachineContext context);
    }

    private static class UnKnownState implements RoleState {

        Integer epoch;

        public UnKnownState(Integer epoch) {
            this.epoch = epoch;
        }

        @Override
        public RoleState transform(StateMachineContext context) {
            MetaDataAdapter adapter = context.adapter();
            Optional<MetaData> metaDataOpt = adapter.query();
            if (!metaDataOpt.isPresent()) {
                context.reset();
                this.epoch = this.epoch == null ? 1 : this.epoch + 1;
                context.epoch(this.epoch);
                return new CandidateState(this.epoch);
            }

            MetaData metaData = metaDataOpt.get();
            context.epoch(metaData.epoch());
            if (metaData.isMaster(context.node())) {
                return new MasterState(metaData);
            } else {
                return new WorkerState(metaData);
            }
        }

        @Override
        public Callback callback(StateMachineCallback callback) {
            return callback::unknown;
        }
    }

    private static class SafeState implements RoleState {

        Integer epoch;

        public SafeState(Integer epoch) {
            this.epoch = epoch;
        }

        @Override
        public RoleState transform(StateMachineContext context) {
            RoleState.heartBeatPark(context);
            return new UnKnownState(this.epoch).transform(context);
        }

        @Override
        public Callback callback(StateMachineCallback callback) {
            return callback::safe;
        }
    }

    private static class MasterState implements RoleState {

        MetaData metaData;

        public MasterState(MetaData metaData) {
            this.metaData = metaData;
        }

        @Override
        public RoleState transform(StateMachineContext context) {
            this.metaData.increaseCount();
            RoleState.heartBeatPark(context);
            if (context.adapter().postDelyIfPresent(this.metaData, -1)) {
                return this;
            }
            context.reset();
            context.epoch(this.metaData.epoch());
            return new UnKnownState(this.metaData.epoch());
        }

        @Override
        public Callback callback(StateMachineCallback callback) {
            return callback::master;
        }
    }

    private static class WorkerState implements RoleState {

        private MetaData metaData;
        private int count = 0;

        public WorkerState(MetaData metaData) {
            this.metaData = metaData;
        }

        @Override
        public RoleState transform(StateMachineContext context) {
            RoleState.heartBeatPark(context);
            RoleState nextState = new UnKnownState(this.metaData.epoch()).transform(context);
            if (nextState instanceof WorkerState) {
                this.merge((WorkerState) nextState);
                if (this.count > context.config().exceedsWorkerCount()) {
                    return new CandidateState(this.metaData.epoch() + 1);
                } else {
                    return this;
                }
            } else {
                return nextState;
            }
        }

        @Override
        public Callback callback(StateMachineCallback callback) {
            return callback::worker;
        }

        public void merge(WorkerState state) {
            if (state.metaData.epoch() > this.metaData.epoch()) {
                this.count = 0;
                this.metaData = state.metaData;
            } else if (state.metaData.epoch() < this.metaData.epoch()){
                throw new IllegalStateException("Epoch must increase");
            } else if (state.metaData.epoch() == this.metaData.epoch() &&
                    state.metaData.count() < this.metaData.count()) {
                throw new IllegalStateException("Meta count must increase");
            } else if (state.metaData.epoch() == this.metaData.epoch() &&
                       state.metaData.count() > this.metaData.count()) {
                this.count = 0;
                this.metaData = state.metaData;
            } else {
                this.count++;
            }
        }
    }

    private static class CandidateState implements RoleState {

        Integer epoch;

        public CandidateState(Integer epoch) {
            this.epoch = epoch;
        }

        @Override
        public RoleState transform(StateMachineContext context) {
            RoleState.randomPark(context);
            int epoch = this.epoch == null ? 1 : this.epoch;
            MetaData metaData = new MetaData(context.config().node(), epoch);
            //failover to master success
            context.epoch(metaData.epoch());
            if (context.adapter().postDelyIfPresent(metaData, -1)) {
                return new MasterState(metaData);
            } else {
                return new WorkerState(metaData);
            }
        }

        @Override
        public Callback callback(StateMachineCallback callback) {
            return callback::candidate;
        }
    }

    private static class StateMachineContextImpl implements StateMachineContext {

        private Integer epoch;
        private final String node;
        private final RoleElectionStateMachineImpl machine;

        public StateMachineContextImpl(RoleElectionStateMachineImpl machine) {
            this.node = machine.config.node();
            this.machine = machine;
        }

        @Override
        public Integer epoch() {
            return this.epoch;
        }

        @Override
        public String node() {
            return this.node;
        }

        @Override
        public void epoch(Integer epoch) {
            this.epoch = epoch;
        }

        @Override
        public MetaDataAdapter adapter() {
            return this.machine.adapter();
        }

        @Override
        public Config config() {
            return this.machine.config;
        }

        @Override
        public RoleElectionStateMachine stateMachine() {
            return this.machine;
        }

        @Override
        public void reset() {
            this.epoch = null;
        }
    }

    protected MetaDataAdapter adapter() {
        return this.metaDataAdapter;
    }
}
