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

package com.baidu.hugegraph.backend.tx;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.Transaction;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.AbstractSerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.iterator.FlatMapperIterator;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.Idfiable;
import com.baidu.hugegraph.type.define.Action;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.Log;
import com.google.common.util.concurrent.RateLimiter;

public abstract class AbstractTransaction implements Transaction {

    protected static final Logger LOG = Log.logger(Transaction.class);

    private final Thread ownerThread = Thread.currentThread();

    private boolean autoCommit = false;
    private boolean closed = false;
    private boolean committing = false;
    private boolean committing2Backend = false;

    private final HugeGraph graph;
    private final BackendStore store;

    private BackendMutation mutation;

    protected final AbstractSerializer serializer;

    public AbstractTransaction(HugeGraph graph, BackendStore store) {
        E.checkNotNull(graph, "graph");
        E.checkNotNull(store, "store");

        this.graph = graph;
        this.serializer = this.graph.serializer();

        this.store = store;
        this.reset();

        store.open(graph.configuration());
    }

    public HugeGraph graph() {
        E.checkNotNull(this.graph, "graph");
        return this.graph;
    }

    public BackendStore store() {
        E.checkNotNull(this.graph, "store");
        return this.store;
    }

    public <R> R metadata(HugeType type, String meta, Object... args) {
        return this.store().metadata(type, meta, args);
    }

    @Watched(prefix = "tx")
    public QueryResults query(Query query) {
        LOG.debug("Transaction query: {}", query);
        /*
         * NOTE: it's dangerous if an IdQuery/ConditionQuery is empty
         * check if the query is empty and its class is not the Query itself
         */
        if (query.empty() && !query.getClass().equals(Query.class)) {
            throw new BackendException("Query without any id or condition");
        }

        Query squery = this.serializer.writeQuery(query);

        this.beforeRead();
        try {
            return new QueryResults(this.store.query(squery), query);
        } finally {
            this.afterRead(); // TODO: not complete the iteration currently
        }
    }

    @Watched(prefix = "tx")
    public BackendEntry query(HugeType type, Id id) {
        IdQuery q = new IdQuery(type, id);
        Iterator<BackendEntry> results = this.query(q).iterator();
        if (results.hasNext()) {
            BackendEntry entry = results.next();
            assert !results.hasNext();
            return entry;
        }
        return null;
    }

    public BackendEntry get(HugeType type, Id id) {
        BackendEntry entry = this.query(type, id);
        if (entry == null) {
            throw new NotFoundException(
                      "Not found the %s entry with id '%s'",
                      type.readableName(), id);
        }
        return entry;
    }

    @Watched(prefix = "tx")
    @Override
    public void commit() throws BackendException {
        LOG.debug("Transaction commit() [auto: {}]...", this.autoCommit);
        this.checkOwnerThread();

        if (this.closed) {
            throw new BackendException("Transaction has been closed");
        }

        if (this.committing) {
            // It is not allowed to recursively commit in a transaction
            return;
        }

        if (!this.hasUpdates()) {
            LOG.debug("Transaction has no data to commit({})", store());
            return;
        }

        // Do rate limit if needed
        RateLimiter rateLimiter = this.graph.rateLimiter();
        if (rateLimiter != null) {
            int size = this.mutationSize();
            assert size > 0;
            double time = rateLimiter.acquire(size);
            if (time > 0) {
                LOG.debug("Waited for {}s to mutate {} item(s)", time, size);
            }
        }

        // Do commit
        assert !this.committing : "Not allowed to commit when it's committing";
        this.committing = true;
        try {
            this.commit2Backend();
        } finally {
            this.committing = false;
            this.reset();
        }
    }

    @Override
    public void commitIfGtSize(int size) throws BackendException {
        if (this.mutationSize() >= size) {
            this.commit();
        }
    }

    @Watched(prefix = "tx")
    @Override
    public void rollback() throws BackendException {
        LOG.debug("Transaction rollback()...");
        this.reset();
        if (this.committing2Backend) {
            this.committing2Backend = false;
            this.store.rollbackTx();
        }
    }

    @Watched(prefix = "tx")
    @Override
    public void close() {
        if (this.hasUpdates()) {
            throw new BackendException("There are still changes to commit");
        }
        if (this.closed) {
            return;
        }
        this.closed = true;
        this.autoCommit = true; /* Let call after close() fail to commit */
        this.store().close();
    }

    @Override
    public boolean autoCommit() {
        return this.autoCommit;
    }

    public boolean hasUpdates() {
        return !this.mutation.isEmpty();
    }

    public int mutationSize() {
        return this.mutation.size();
    }

    protected void autoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    protected void reset() {
        this.mutation = new BackendMutation();
    }

    protected BackendMutation mutation() {
        return this.mutation;
    }

    protected void commit2Backend() {
        BackendMutation mutation = this.prepareCommit();
        assert !mutation.isEmpty();
        this.commitMutation2Backend(mutation);
    }

    protected void commitMutation2Backend(BackendMutation... mutations) {
        assert mutations.length > 0;
        this.committing2Backend = true;

        // If an exception occurred, catch in the upper layer and rollback
        this.store.beginTx();
        for (BackendMutation mutation : mutations) {
            this.store.mutate(mutation);
        }
        this.store.commitTx();

        this.committing2Backend = false;
    }

    protected BackendMutation prepareCommit() {
        // For sub-class preparing data, nothing to do here
        LOG.debug("Transaction prepareCommit()...");
        return this.mutation();
    }

    protected void beforeWrite() {
        // TODO: auto open()
    }

    protected void afterWrite() {
        if (this.autoCommit()) {
            this.commitOrRollback();
        }
    }

    protected void beforeRead() {
        if (this.autoCommit() && this.hasUpdates()) {
            this.commitOrRollback();
        }
    }

    protected void afterRead() {
        // pass
    }

    @Watched(prefix = "tx")
    protected void commitOrRollback() {
        LOG.debug("Transaction commitOrRollback()");
        this.checkOwnerThread();

        /*
         * The mutation will be reset after commit, in order to log the
         * mutation after failure, let's save it to a local variable.
         */
        BackendMutation mutation = this.mutation();

        try {
            // Do commit
            this.commit();
        } catch (Throwable e1) {
            LOG.error("Failed to commit changes:", e1);
            // Do rollback
            try {
                this.rollback();
            } catch (Throwable e2) {
                LOG.error("Failed to rollback changes:\n {}", mutation, e2);
            }
            // Rethrow the commit exception
            throw new BackendException(
                      "Failed to commit changes: %s", e1.getMessage());
        }
    }

    protected void checkOwnerThread() {
        if (Thread.currentThread() != this.ownerThread) {
            throw new BackendException("Can't operate a tx in other threads");
        }
    }

    @Watched(prefix = "tx")
    public void doInsert(BackendEntry entry) {
        this.doAction(Action.INSERT, entry);
    }

    @Watched(prefix = "tx")
    public void doAppend(BackendEntry entry) {
        this.doAction(Action.APPEND, entry);
    }

    @Watched(prefix = "tx")
    public void doEliminate(BackendEntry entry) {
        this.doAction(Action.ELIMINATE, entry);
    }

    @Watched(prefix = "tx")
    public void doRemove(BackendEntry entry) {
        this.doAction(Action.DELETE, entry);
    }

    protected void doAction(Action action, BackendEntry entry) {
        LOG.debug("Transaction {} entry {}", action, entry);
        E.checkNotNull(entry, "entry");
        this.mutation.add(entry, action);
    }

    public static class QueryResults {

        private static final QueryResults EMPTY = new QueryResults(
                                                  Collections.emptyIterator(),
                                                  Query.NONE);

        private final Iterator<BackendEntry> results;
        private final List<Query> queries;

        public QueryResults(Iterator<BackendEntry> results, Query query) {
            this(results);
            this.addQuery(query);
        }

        public QueryResults(Iterator<BackendEntry> results) {
            this.results = results;
            this.queries = InsertionOrderUtil.newList();
        }

        public void setQuery(Query query) {
            if (this.queries.size() > 0) {
                this.queries.clear();
            }
            this.addQuery(query);
        }

        private void addQuery(Query query) {
            E.checkNotNull(query, "query");
            this.queries.add(query);
        }

        private void addQueries(List<Query> queries) {
            for (Query query : queries) {
                this.addQuery(query);
            }
        }

        public Iterator<BackendEntry> iterator() {
            return this.results;
        }

        public List<BackendEntry> list() {
            return IteratorUtils.list(this.results);
        }

        public List<Query> queries() {
            return Collections.unmodifiableList(this.queries);
        }

        protected <T extends Idfiable> Iterator<T> keepInputOrderIfNeeded(
                                                   Iterator<T> origin) {
            if (!origin.hasNext()) {
                // Empty result found
                return origin;
            }
            Set<Id> ids;
            if (this.paging() || !this.mustSortByInputIds() ||
                (ids = this.queryIds()).size() <= 1) {
                /*
                 * Return the original iterator if it's paging query or if the
                 * query input is less than one id, or don't have to do sort.
                 */
                return origin;
            }

            // Fill map with all elements
            Map<Id, T> results = new HashMap<>();
            fillMap(origin, results);

            return new MapperIterator<>(ids.iterator(), id -> {
                return results.get(id);
            });
        }

        private boolean mustSortByInputIds() {
            if (this.queries.size() == 1) {
                Query query = this.queries.get(0);
                if (query instanceof IdQuery) {
                    return ((IdQuery) query).mustSortByInput();
                }
            }
            return true;
        }

        private boolean paging() {
            for (Query query : this.queries) {
                Query origin = query.originQuery();
                if (query.paging() || origin != null && origin.paging()) {
                    return true;
                }
            }
            return false;
        }

        private Set<Id> queryIds() {
            if (this.queries.size() == 1) {
                return this.queries.get(0).ids();
            }

            Set<Id> ids = InsertionOrderUtil.newSet();
            for (Query query : this.queries) {
                ids.addAll(query.ids());
            }
            return ids;
        }

        public static <T extends Idfiable> void fillMap(Iterator<T> iterator,
                                                        Map<Id, T> map) {
            while (iterator.hasNext()) {
                T result = iterator.next();
                assert result.id() != null;
                map.put(result.id(), result);
            }
        }

        public static QueryResults empty() {
            return EMPTY;
        }

        public static <T> QueryResults flatMap(Iterator<T> iterator,
                                               Function<T, QueryResults> func) {
            QueryResults[] qr = new QueryResults[1];
            qr[0] = new QueryResults(new FlatMapperIterator<>(iterator, i -> {
                QueryResults results = func.apply(i);
                if (results == null) {
                    return null;
                }
                qr[0].addQueries(results.queries());
                return results.iterator();
            }));
            return qr[0];
        }
    }
}
