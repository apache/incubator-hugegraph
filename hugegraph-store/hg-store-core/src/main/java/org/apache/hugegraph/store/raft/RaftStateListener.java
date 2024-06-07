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

package org.apache.hugegraph.store.raft;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftException;

public interface RaftStateListener {

    /**
     * Called when current node becomes leader.
     *
     * @param newTerm the new term
     */
    void onLeaderStart(final long newTerm);

    /**
     * Called when current node loses leadership.
     *
     * @param oldTerm the old term
     */
    default void onLeaderStop(final long oldTerm) {
    }

    /**
     * This method is called when a follower or candidate starts following a leader and its leaderId
     * (should be NULL before the method is called) is set to the leader's id, situations including:
     * 1. A candidate receives appendEntries request from a leader
     * 2. A follower(without leader) receives appendEntries from a leader
     * <p>
     * The parameters gives the information(leaderId and term) about the very
     * leader whom the follower starts to follow.
     * User can reset the node's information as it starts to follow some leader.
     *
     * @param newLeaderId the new leader id whom the follower starts to follow
     * @param newTerm     the new term
     */
    default void onStartFollowing(final PeerId newLeaderId, final long newTerm) {
    }

    /**
     * This method is called when a follower stops following a leader and its leaderId becomes null,
     * situations including:
     * 1. Handle election timeout and start preVote
     * 2. Receive requests with higher term such as VoteRequest from a candidate
     * or appendEntries request from a new leader
     * 3. Receive timeoutNow request from current leader and start request vote.
     * <p>
     * The parameters gives the information(leaderId and term) about the very leader
     * whom the follower followed before.
     * User can reset the node's information as it stops following some leader.
     *
     * @param oldLeaderId the old leader id whom the follower followed before
     * @param oldTerm     the old term
     */
    default void onStopFollowing(final PeerId oldLeaderId, final long oldTerm) {
    }

    /**
     * Invoked when a configuration has been committed to the group.
     *
     * @param conf committed configuration
     */
    default void onConfigurationCommitted(final Configuration conf) {
    }

    default void onDataCommitted(long index) {
    }

    void onError(final RaftException e);
}
