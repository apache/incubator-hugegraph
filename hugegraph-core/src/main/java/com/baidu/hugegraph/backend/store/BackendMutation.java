package com.baidu.hugegraph.backend.store;

import java.util.ArrayList;
import java.util.Collection;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class BackendMutation {

    private Collection<BackendEntry> additions;
    private Collection<BackendEntry> deletions;

    public BackendMutation(Collection<BackendEntry> additions) {
        this(additions, null);
    }

    public BackendMutation(Collection<BackendEntry> additions,
            Collection<BackendEntry> deletions) {
        this.additions = additions;
        this.deletions = deletions;
    }

    /**
     * Whether this mutation has additions
     *
     * @return boolean
     */
    public boolean hasAdditions() {
        return this.additions != null && !this.additions.isEmpty();
    }

    /**
     * Whether this mutation has deletions
     *
     * @return boolean
     */
    public boolean hasDeletions() {
        return this.deletions != null && !this.deletions.isEmpty();
    }

    /**
     * Whether this mutation is empty
     *
     * @return boolean
     */
    public boolean isEmpty() {
        return !hasAdditions() && !hasDeletions();
    }

    /**
     * Returns the list of additions in this mutation
     *
     * @return
     */
    public Collection<BackendEntry> additions() {
        if (this.additions == null) {
            return ImmutableList.of();
        }
        return this.additions;
    }

    /**
     * Returns the list of deletions in this mutation.
     *
     * @return
     */
    public Collection<BackendEntry> deletions() {
        if (this.deletions == null) {
            return ImmutableList.of();
        }
        return this.deletions;
    }

    /**
     * Adds a new entry as an addition to this mutation
     *
     * @param entry
     */
    public void addition(BackendEntry entry) {
        if (this.additions == null) {
            this.additions = new ArrayList<>();
        }
        this.additions.add(entry);
    }

    /**
     * Adds a new entry as a deletion to this mutation
     *
     * @param entry
     */
    public void deletion(BackendEntry entry) {
        if (this.deletions == null) {
            this.deletions = new ArrayList<>();
        }
        this.deletions.add(entry);
    }

    /**
     * Merges another mutation into this mutation. Ensures that all additions and deletions
     * are added to this mutation. Does not remove duplicates if such exist - this needs to be ensured by the caller.
     *
     * @param m
     */
    public void merge(BackendMutation m) {
        Preconditions.checkNotNull(m);

        if (null != m.additions) {
            if (null == this.additions) {
                this.additions = m.additions;
            } else {
                this.additions.addAll(m.additions);
            }
        }

        if (null != m.deletions) {
            if (null == this.deletions) {
                this.deletions = m.deletions;
            } else {
                this.deletions.addAll(m.deletions);
            }
        }
    }

}
