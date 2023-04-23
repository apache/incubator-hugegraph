package com.baidu.hugegraph.store.raft;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;

import java.io.File;

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
    default void onLeaderStop(final long oldTerm){};

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
    default void onStartFollowing(final PeerId newLeaderId, final long newTerm){};

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
    default void onStopFollowing(final PeerId oldLeaderId, final long oldTerm){};


    /**
     * Invoked when a configuration has been committed to the group.
     *
     * @param conf committed configuration
     */
    default void onConfigurationCommitted(final Configuration conf){};

    default void onDataCommitted(long index){};
    void onError(final RaftException e);
}
