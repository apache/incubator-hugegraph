/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.masterelection;

import java.security.SecureRandom;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class StandardRoleElectionStateMachine implements RoleElectionStateMachine {

    private static final Logger LOG = Log.logger(StandardRoleElectionStateMachine.class);

    private final Config config;
    private final ClusterRoleStore roleStore;
    private final ExecutorService applyThread;

    private volatile boolean shutdown;
    private volatile RoleState state;

    public StandardRoleElectionStateMachine(Config config, ClusterRoleStore roleStore) {
        this.config = config;
        this.roleStore = roleStore;
        this.applyThread = Executors.newSingleThreadExecutor();
        this.state = new UnknownState(null);
        this.shutdown = false;
    }

    @Override
    public void shutdown() {
        if (this.shutdown) {
            return;
        }
        this.shutdown = true;
        this.applyThread.shutdown();
    }

    @Override
    public void start(RoleListener stateMachineCallback) {
        this.applyThread.execute(() -> this.apply(stateMachineCallback));
    }

    private void apply(RoleListener stateMachineCallback) {
        int failCount = 0;
        StateMachineContextImpl context = new StateMachineContextImpl(this);
        while (!this.shutdown) {
            E.checkArgumentNotNull(this.state, "State don't be null");
            try {
                RoleState pre = this.state;
                this.state = state.transform(context);
                LOG.trace("server {} epoch {} role state change {} to {}",
                          context.node(), context.epoch(), pre.getClass().getSimpleName(),
                          this.state.getClass().getSimpleName());
                Callback runnable = this.state.callback(stateMachineCallback);
                runnable.call(context);
                failCount = 0;
            } catch (Throwable e) {
                stateMachineCallback.error(context, e);
                failCount++;
                if (failCount >= this.config.exceedsFailCount()) {
                    this.state = new AbdicationState(context.epoch());
                    Callback runnable = this.state.callback(stateMachineCallback);
                    runnable.call(context);
                }
            }
        }
    }

    protected ClusterRoleStore roleStore() {
        return this.roleStore;
    }

    private interface RoleState {

        SecureRandom SECURE_RANDOM = new SecureRandom();

        RoleState transform(StateMachineContext context);

        Callback callback(RoleListener callback);

        static void heartBeatPark(StateMachineContext context) {
            long heartBeatIntervalSecond = context.config().heartBeatIntervalSecond();
            LockSupport.parkNanos(heartBeatIntervalSecond * 1_000_000_000);
        }

        static void randomPark(StateMachineContext context) {
            long randomTimeout = context.config().randomTimeoutMillisecond();
            long baseTime = context.config().baseTimeoutMillisecond();
            long timeout = (long) (baseTime + (randomTimeout / 10.0 * SECURE_RANDOM.nextInt(11)));
            LockSupport.parkNanos(timeout * 1_000_000);
        }
    }

    @FunctionalInterface
    private interface Callback {

        void call(StateMachineContext context);
    }

    private static class UnknownState implements RoleState {

        final Integer epoch;

        public UnknownState(Integer epoch) {
            this.epoch = epoch;
        }

        @Override
        public RoleState transform(StateMachineContext context) {
            ClusterRoleStore adapter = context.roleStore();
            Optional<ClusterRole> clusterRoleOpt = adapter.query();
            if (!clusterRoleOpt.isPresent()) {
                context.reset();
                Integer nextEpoch = this.epoch == null ? 1 : this.epoch + 1;
                context.epoch(nextEpoch);
                return new CandidateState(nextEpoch);
            }

            ClusterRole clusterRole = clusterRoleOpt.get();
            if (this.epoch != null && clusterRole.epoch() < this.epoch) {
                context.reset();
                Integer nextEpoch = this.epoch + 1;
                context.epoch(nextEpoch);
                return new CandidateState(nextEpoch);
            }

            context.epoch(clusterRole.epoch());
            context.master(new MasterServerInfoImpl(clusterRole.node(), clusterRole.url()));
            if (clusterRole.isMaster(context.node())) {
                return new MasterState(clusterRole);
            } else {
                return new WorkerState(clusterRole);
            }
        }

        @Override
        public Callback callback(RoleListener callback) {
            return callback::unknown;
        }
    }

    private static class AbdicationState implements RoleState {

        private final Integer epoch;

        public AbdicationState(Integer epoch) {
            this.epoch = epoch;
        }

        @Override
        public RoleState transform(StateMachineContext context) {
            context.master(null);
            RoleState.heartBeatPark(context);
            return new UnknownState(this.epoch).transform(context);
        }

        @Override
        public Callback callback(RoleListener callback) {
            return callback::onAsRoleAbdication;
        }
    }

    private static class MasterState implements RoleState {

        private final ClusterRole clusterRole;

        public MasterState(ClusterRole clusterRole) {
            this.clusterRole = clusterRole;
        }

        @Override
        public RoleState transform(StateMachineContext context) {
            this.clusterRole.increaseClock();
            RoleState.heartBeatPark(context);
            if (context.roleStore().updateIfNodePresent(this.clusterRole)) {
                return this;
            }
            context.reset();
            context.epoch(this.clusterRole.epoch());
            return new UnknownState(this.clusterRole.epoch()).transform(context);
        }

        @Override
        public Callback callback(RoleListener callback) {
            return callback::onAsRoleMaster;
        }
    }

    private static class WorkerState implements RoleState {

        private ClusterRole clusterRole;
        private int clock;

        public WorkerState(ClusterRole clusterRole) {
            this.clusterRole = clusterRole;
            this.clock = 0;
        }

        @Override
        public RoleState transform(StateMachineContext context) {
            RoleState.heartBeatPark(context);
            RoleState nextState = new UnknownState(this.clusterRole.epoch()).transform(context);
            if (nextState instanceof WorkerState) {
                this.merge((WorkerState) nextState);
                if (this.clock > context.config().masterDeadTimes()) {
                    return new CandidateState(this.clusterRole.epoch() + 1);
                } else {
                    return this;
                }
            } else {
                return nextState;
            }
        }

        @Override
        public Callback callback(RoleListener callback) {
            return callback::onAsRoleWorker;
        }

        public void merge(WorkerState state) {
            if (state.clusterRole.epoch() > this.clusterRole.epoch()) {
                this.clock = 0;
                this.clusterRole = state.clusterRole;
            } else if (state.clusterRole.epoch() < this.clusterRole.epoch()) {
                throw new IllegalStateException("Epoch must increase");
            } else if (state.clusterRole.epoch() == this.clusterRole.epoch() &&
                       state.clusterRole.clock() < this.clusterRole.clock()) {
                throw new IllegalStateException("Clock must increase");
            } else if (state.clusterRole.epoch() == this.clusterRole.epoch() &&
                       state.clusterRole.clock() > this.clusterRole.clock()) {
                this.clock = 0;
                this.clusterRole = state.clusterRole;
            } else {
                this.clock++;
            }
        }
    }

    private static class CandidateState implements RoleState {

        private final Integer epoch;

        public CandidateState(Integer epoch) {
            this.epoch = epoch;
        }

        @Override
        public RoleState transform(StateMachineContext context) {
            RoleState.randomPark(context);
            int epoch = this.epoch == null ? 1 : this.epoch;
            ClusterRole clusterRole = new ClusterRole(context.config().node(),
                                                      context.config().url(), epoch);
            // The master failover completed
            context.epoch(clusterRole.epoch());
            if (context.roleStore().updateIfNodePresent(clusterRole)) {
                context.master(new MasterServerInfoImpl(clusterRole.node(), clusterRole.url()));
                return new MasterState(clusterRole);
            } else {
                return new UnknownState(epoch).transform(context);
            }
        }

        @Override
        public Callback callback(RoleListener callback) {
            return callback::onAsRoleCandidate;
        }
    }

    private static class StateMachineContextImpl implements StateMachineContext {

        private Integer epoch;
        private final String node;
        private final StandardRoleElectionStateMachine machine;

        private MasterServerInfo masterServerInfo;

        public StateMachineContextImpl(StandardRoleElectionStateMachine machine) {
            this.node = machine.config.node();
            this.machine = machine;
        }

        @Override
        public void master(MasterServerInfo info) {
            this.masterServerInfo = info;
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
        public ClusterRoleStore roleStore() {
            return this.machine.roleStore();
        }

        @Override
        public Config config() {
            return this.machine.config;
        }

        @Override
        public MasterServerInfo master() {
            return this.masterServerInfo;
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

    private static class MasterServerInfoImpl implements StateMachineContext.MasterServerInfo {

        private final String node;
        private final String url;

        public MasterServerInfoImpl(String node, String url) {
            this.node = node;
            this.url = url;
        }

        @Override
        public String url() {
            return this.url;
        }

        @Override
        public String node() {
            return this.node;
        }
    }
}
