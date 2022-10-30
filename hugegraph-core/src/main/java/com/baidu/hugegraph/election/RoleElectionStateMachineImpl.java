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

    private volatile boolean shutdown;
    private final Config config;
    private volatile RoleState state;
    private final RoleTypeDataAdapter roleTypeDataAdapter;

    public RoleElectionStateMachineImpl(Config config, RoleTypeDataAdapter adapter) {
        this.config = config;
        this.roleTypeDataAdapter = adapter;
        this.state = new UnknownState(null);
        this.shutdown = false;
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
                failCount++;
                if (failCount >= this.config.exceedsFailCount()) {
                    this.state = new AbdicationState(context.epoch());
                    Callback runnable = this.state.callback(stateMachineCallback);
                    runnable.call(context);
                }
            }
        }
    }

    private interface RoleState {

        SecureRandom SECURE_RANDOM = new SecureRandom();

        RoleState transform(StateMachineContext context);

        Callback callback(StateMachineCallback callback);

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
            RoleTypeDataAdapter adapter = context.adapter();
            Optional<RoleTypeData> roleTypeDataOpt = adapter.query();
            if (!roleTypeDataOpt.isPresent()) {
                context.reset();
                Integer nextEpoch = this.epoch == null ? 1 : this.epoch + 1;
                context.epoch(nextEpoch);
                return new CandidateState(nextEpoch);
            }

            RoleTypeData roleTypeData = roleTypeDataOpt.get();
            if (this.epoch != null && roleTypeData.epoch() < this.epoch) {
                context.reset();
                Integer nextEpoch = this.epoch + 1;
                context.epoch(nextEpoch);
                return new CandidateState(nextEpoch);
            }

            context.epoch(roleTypeData.epoch());
            if (roleTypeData.isMaster(context.node())) {
                return new MasterState(roleTypeData);
            } else {
                return new WorkerState(roleTypeData);
            }
        }

        @Override
        public Callback callback(StateMachineCallback callback) {
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
            RoleState.heartBeatPark(context);
            return new UnknownState(this.epoch).transform(context);
        }

        @Override
        public Callback callback(StateMachineCallback callback) {
            return callback::abdication;
        }
    }

    private static class MasterState implements RoleState {

        private final RoleTypeData roleTypeData;

        public MasterState(RoleTypeData roleTypeData) {
            this.roleTypeData = roleTypeData;
        }

        @Override
        public RoleState transform(StateMachineContext context) {
            this.roleTypeData.increaseClock();
            RoleState.heartBeatPark(context);
            if (context.adapter().updateIfNodePresent(this.roleTypeData)) {
                return this;
            }
            context.reset();
            context.epoch(this.roleTypeData.epoch());
            return new UnknownState(this.roleTypeData.epoch()).transform(context);
        }

        @Override
        public Callback callback(StateMachineCallback callback) {
            return callback::master;
        }
    }

    private static class WorkerState implements RoleState {

        private RoleTypeData roleTypeData;
        private int clock;

        public WorkerState(RoleTypeData roleTypeData) {
            this.roleTypeData = roleTypeData;
            this.clock = 0;
        }

        @Override
        public RoleState transform(StateMachineContext context) {
            RoleState.heartBeatPark(context);
            RoleState nextState = new UnknownState(this.roleTypeData.epoch()).transform(context);
            if (nextState instanceof WorkerState) {
                this.merge((WorkerState) nextState);
                if (this.clock > context.config().exceedsWorkerCount()) {
                    return new CandidateState(this.roleTypeData.epoch() + 1);
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
            if (state.roleTypeData.epoch() > this.roleTypeData.epoch()) {
                this.clock = 0;
                this.roleTypeData = state.roleTypeData;
            } else if (state.roleTypeData.epoch() < this.roleTypeData.epoch()) {
                throw new IllegalStateException("Epoch must increase");
            } else if (state.roleTypeData.epoch() == this.roleTypeData.epoch() &&
                       state.roleTypeData.clock() < this.roleTypeData.clock()) {
                throw new IllegalStateException("Clock must increase");
            } else if (state.roleTypeData.epoch() == this.roleTypeData.epoch() &&
                       state.roleTypeData.clock() > this.roleTypeData.clock()) {
                this.clock = 0;
                this.roleTypeData = state.roleTypeData;
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
            RoleTypeData roleTypeData = new RoleTypeData(context.config().node(), epoch);
            //failover to master success
            context.epoch(roleTypeData.epoch());
            if (context.adapter().updateIfNodePresent(roleTypeData)) {
                return new MasterState(roleTypeData);
            } else {
                return new UnknownState(epoch).transform(context);
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
        public RoleTypeDataAdapter adapter() {
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

    protected RoleTypeDataAdapter adapter() {
        return this.roleTypeDataAdapter;
    }
}
