package com.baidu.hugegraph.backend.store;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Created by jishilei on 17/3/19.
 */
public abstract class Mutation<E, K> {

    private List<E> additions;

    private List<K> deletions;

    public Mutation(List<E> additions, List<K> deletions) {
        Preconditions.checkNotNull(additions);
        Preconditions.checkNotNull(deletions);
        if (additions.isEmpty()) {
            this.additions = null;
        } else {
            this.additions = Lists.newArrayList(additions);
        }
        if (deletions.isEmpty()) {
            this.deletions = null;
        } else {
            this.deletions = Lists.newArrayList(deletions);
        }
    }

    public Mutation() {
        this.additions = null;
        this.deletions = null;
    }

    /**
     * Whether this mutation has additions
     *
     * @return
     */
    public boolean hasAdditions() {
        return additions != null && !additions.isEmpty();
    }

    /**
     * Whether this mutation has deletions
     *
     * @return
     */
    public boolean hasDeletions() {
        return deletions != null && !deletions.isEmpty();
    }

    /**
     * Returns the list of additions in this mutation
     *
     * @return
     */
    public List<E> getAdditions() {
        if (additions == null) {
            return ImmutableList.of();
        }
        return additions;
    }

    /**
     * Returns the list of deletions in this mutation.
     *
     * @return
     */
    public List<K> getDeletions() {
        if (deletions == null) {
            return ImmutableList.of();
        }
        return deletions;
    }

    /**
     * Adds a new entry as an addition to this mutation
     *
     * @param entry
     */
    public void addition(E entry) {
        if (additions == null) {
            additions = new ArrayList<E>();
        }
        additions.add(entry);
    }

    /**
     * Adds a new key as a deletion to this mutation
     *
     * @param key
     */
    public void deletion(K key) {
        if (deletions == null) {
            deletions = new ArrayList<K>();
        }
        deletions.add(key);
    }

    /**
     * Merges another mutation into this mutation. Ensures that all additions and deletions
     * are added to this mutation. Does not remove duplicates if such exist - this needs to be ensured by the caller.
     *
     * @param m
     */
    public void merge(Mutation<E, K> m) {
        Preconditions.checkNotNull(m);

        if (null != m.additions) {
            if (null == additions) {
                additions = m.additions;
            } else {
                additions.addAll(m.additions);
            }
        }

        if (null != m.deletions) {
            if (null == deletions) {
                deletions = m.deletions;
            } else {
                deletions.addAll(m.deletions);
            }
        }
    }

    public boolean isEmpty() {
        return getTotalMutations() == 0;
    }

    public int getTotalMutations() {
        return (additions == null ? 0 : additions.size()) + (deletions == null ? 0 : deletions.size());
    }

}
