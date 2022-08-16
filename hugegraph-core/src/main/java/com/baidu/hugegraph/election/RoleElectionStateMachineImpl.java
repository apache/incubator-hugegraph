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

package com.baidu.hugegraph.election;

import java.security.SecureRandom;
import java.util.Optional;
import java.util.concurrent.locks.LockSupport;

import com.baidu.hugegraph.util.E;

public class RoleElectionStateMachineImpl implements RoleElectionStateMachine {

    private volatile boolean shutdown = false;
    private final Config config;
    private volatile RoleState state;
    private final RoleStataDataAdapter roleStataDataAdapter;

    public RoleElectionStateMachineImpl(Config config, RoleStataDataAdapter adapter) {
        this.config = config;
        this.roleStataDataAdapter = adapter;
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

        SecureRandom secureRandom = new SecureRandom();

        RoleState transform(StateMachineContext context);

        Callback callback(StateMachineCallback callback);

        static void heartBeatPark(StateMachineContext context) {
            long heartBeatIntervalSecond = context.config().heartBeatIntervalSecond();
            LockSupport.parkNanos(heartBeatIntervalSecond * 1_000_000_000);
        }

        static void randomPark(StateMachineContext context) {
            long randomTimeout = context.config().randomTimeoutMillisecond();
            long baseTime = context.config().baseTimeoutMillisecond();
            long timeout = (long) (baseTime + (randomTimeout / 10.0 * secureRandom.nextInt(11)));
            LockSupport.parkNanos(timeout * 1_000_000);
        }
    }

    @FunctionalInterface
    private interface Callback {

        void call(StateMachineContext context);
    }

    private static class UnKnownState implements RoleState {

        final Integer epoch;

        public UnKnownState(Integer epoch) {
            this.epoch = epoch;
        }

        @Override
        public RoleState transform(StateMachineContext context) {
            RoleStataDataAdapter adapter = context.adapter();
            Optional<RoleStateData> stateDataOpt = adapter.query();
            if (!stateDataOpt.isPresent()) {
                context.reset();
                Integer nextEpoch = this.epoch == null ? 1 : this.epoch + 1;
                context.epoch(nextEpoch);
                return new CandidateState(nextEpoch);
            }

            RoleStateData stateData = stateDataOpt.get();
            if (this.epoch != null && stateData.epoch() < this.epoch) {
                context.reset();
                Integer nextEpoch = this.epoch + 1;
                context.epoch(nextEpoch);
                return new CandidateState(nextEpoch);
            }

            context.epoch(stateData.epoch());
            if (stateData.isMaster(context.node())) {
                return new MasterState(stateData);
            } else {
                return new WorkerState(stateData);
            }
        }

        @Override
        public Callback callback(StateMachineCallback callback) {
            return callback::unknown;
        }
    }

    private static class SafeState implements RoleState {

        private final Integer epoch;

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

        private final RoleStateData stateData;

        public MasterState(RoleStateData stateData) {
            this.stateData = stateData;
        }

        @Override
        public RoleState transform(StateMachineContext context) {
            this.stateData.increaseCount();
            RoleState.heartBeatPark(context);
            if (context.adapter().delayIfNodePresent(this.stateData, -1)) {
                return this;
            }
            context.reset();
            context.epoch(this.stateData.epoch());
            return new UnKnownState(this.stateData.epoch()).transform(context);
        }

        @Override
        public Callback callback(StateMachineCallback callback) {
            return callback::master;
        }
    }

    private static class WorkerState implements RoleState {

        private RoleStateData stateData;
        private int count = 0;

        public WorkerState(RoleStateData stateData) {
            this.stateData = stateData;
        }

        @Override
        public RoleState transform(StateMachineContext context) {
            RoleState.heartBeatPark(context);
            RoleState nextState = new UnKnownState(this.stateData.epoch()).transform(context);
            if (nextState instanceof WorkerState) {
                this.merge((WorkerState) nextState);
                if (this.count > context.config().exceedsWorkerCount()) {
                    return new CandidateState(this.stateData.epoch() + 1);
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
            if (state.stateData.epoch() > this.stateData.epoch()) {
                this.count = 0;
                this.stateData = state.stateData;
            } else if (state.stateData.epoch() < this.stateData.epoch()){
                throw new IllegalStateException("Epoch must increase");
            } else if (state.stateData.epoch() == this.stateData.epoch() &&
                       state.stateData.count() < this.stateData.count()) {
                throw new IllegalStateException("Meta count must increase");
            } else if (state.stateData.epoch() == this.stateData.epoch() &&
                       state.stateData.count() > this.stateData.count()) {
                this.count = 0;
                this.stateData = state.stateData;
            } else {
                this.count++;
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
            RoleStateData stateData = new RoleStateData(context.config().node(), epoch);
            //failover to master success
            context.epoch(stateData.epoch());
            if (context.adapter().delayIfNodePresent(stateData, -1)) {
                return new MasterState(stateData);
            } else {
                return new WorkerState(stateData);
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
        public RoleStataDataAdapter adapter() {
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

    protected RoleStataDataAdapter adapter() {
        return this.roleStataDataAdapter;
    }
}
