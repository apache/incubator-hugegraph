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

package com.baidu.hugegraph.job.algorithm;


import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.iterator.FilterIterator;
import com.baidu.hugegraph.iterator.FlatMapperIterator;
import com.baidu.hugegraph.job.Job;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;

import jersey.repackaged.com.google.common.base.Objects;

@SuppressWarnings("deprecation") // StringEscapeUtils
public abstract class AbstractAlgorithm implements Algorithm {

    public static final long MAX_RESULT_SIZE = 100L * Bytes.MB;
    public static final long MAX_QUERY_LIMIT = 100000000L; // about 100GB
    public static final int BATCH = 500;

    public static final String CATEGORY_AGGR = "aggregate";
    public static final String CATEGORY_PATH = "path";
    public static final String CATEGORY_RANK = "rank";
    public static final String CATEGORY_SIMI = "similarity";
    public static final String CATEGORY_COMM = "community";
    public static final String CATEGORY_CENT = "centrality";

    public static final String KEY_DIRECTION = "direction";
    public static final String KEY_LABEL = "label";
    public static final String KEY_DEPTH = "depth";
    public static final String KEY_DEGREE = "degree";
    public static final String KEY_SAMPLE = "sample";
    public static final String KEY_SOURCE_SAMPLE = "source_sample";
    public static final String KEY_SOURCE_LABEL = "source_label";
    public static final String KEY_SOURCE_CLABEL = "source_clabel";
    public static final String KEY_TOP = "top";
    public static final String KEY_TIMES = "times";
    public static final String KEY_STABLE_TIMES = "stable_times";
    public static final String KEY_PRECISION = "precision";
    public static final String KEY_SHOW_MOD= "show_modularity";
    public static final String KEY_SHOW_COMM = "show_community";
    public static final String KEY_CLEAR = "clear";
    public static final String KEY_CAPACITY = "capacity";
    public static final String KEY_LIMIT = "limit";
    public static final String KEY_ALPHA = "alpha";
    public static final String KEY_WORKERS = "workers";

    public static final long DEFAULT_CAPACITY = 10000000L;
    public static final long DEFAULT_LIMIT = 100L;
    public static final long DEFAULT_DEGREE = 100L;
    public static final long DEFAULT_SAMPLE = 1L;
    public static final long DEFAULT_TIMES = 20L;
    public static final long DEFAULT_STABLE_TIMES= 3L;
    public static final double DEFAULT_PRECISION = 1.0 / 1000;
    public static final double DEFAULT_ALPHA = 0.5D;

    public static final String C_LABEL = "c_label";

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        E.checkArgument(parameters.isEmpty(),
                        "Unnecessary parameters: %s", parameters);
    }

    protected static int depth(Map<String, Object> parameters) {
        int depth = parameterInt(parameters, KEY_DEPTH);
        E.checkArgument(depth > 0,
                        "The value of %s must be > 0, but got %s",
                        KEY_DEPTH, depth);
        return depth;
    }

    protected static String edgeLabel(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_LABEL)) {
            return null;
        }
        return parameterString(parameters, KEY_LABEL);
    }

    protected static Directions direction(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_DIRECTION)) {
            return Directions.BOTH;
        }
        Object direction = parameter(parameters, KEY_DIRECTION);
        return parseDirection(direction);
    }

    protected static double alpha(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_ALPHA)) {
            return DEFAULT_ALPHA;
        }
        double alpha = parameterDouble(parameters, KEY_ALPHA);
        checkAlpha(alpha);
        return alpha;
    }

    public static void checkAlpha(double alpha) {
        E.checkArgument(alpha > 0 && alpha <= 1.0,
                        "The alpha of must be in range (0, 1], but got %s",
                        alpha);
    }

    protected static long top(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_TOP)) {
            return 0L;
        }
        long top = parameterLong(parameters, KEY_TOP);
        E.checkArgument(top >= 0L,
                        "The value of %s must be >= 0, but got %s",
                        KEY_TOP, top);
        return top;
    }

    protected static long degree(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_DEGREE)) {
            return DEFAULT_DEGREE;
        }
        long degree = parameterLong(parameters, KEY_DEGREE);
        HugeTraverser.checkDegree(degree);
        return degree;
    }

    protected static long capacity(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_CAPACITY)) {
            return DEFAULT_CAPACITY;
        }
        long capacity = parameterLong(parameters, KEY_CAPACITY);
        HugeTraverser.checkCapacity(capacity);
        return capacity;
    }

    protected static long limit(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_LIMIT)) {
            return DEFAULT_LIMIT;
        }
        long limit = parameterLong(parameters, KEY_LIMIT);
        HugeTraverser.checkLimit(limit);
        return limit;
    }

    protected static long sample(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_SAMPLE)) {
            return DEFAULT_SAMPLE;
        }
        long sample = parameterLong(parameters, KEY_SAMPLE);
        HugeTraverser.checkPositiveOrNoLimit(sample, KEY_SAMPLE);
        return sample;
    }

    protected static long sourceSample(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_SOURCE_SAMPLE)) {
            return HugeTraverser.NO_LIMIT;
        }
        long sample = parameterLong(parameters, KEY_SOURCE_SAMPLE);
        HugeTraverser.checkPositiveOrNoLimit(sample, KEY_SOURCE_SAMPLE);
        return sample;
    }

    protected static String sourceLabel(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_SOURCE_LABEL)) {
            return null;
        }
        return parameterString(parameters, KEY_SOURCE_LABEL);
    }

    protected static String sourceCLabel(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_SOURCE_CLABEL)) {
            return null;
        }
        return parameterString(parameters, KEY_SOURCE_CLABEL);
    }

    protected static int workers(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_WORKERS)) {
            return -1;
        }
        int workers = parameterInt(parameters, KEY_WORKERS);
        HugeTraverser.checkNonNegativeOrNoLimit(workers, KEY_WORKERS);
        return workers;
    }

    public static Object parameter(Map<String, Object> parameters, String key) {
        Object value = parameters.get(key);
        E.checkArgument(value != null,
                        "Expect '%s' in parameters: %s",
                        key, parameters);
        return value;
    }

    public static String parameterString(Map<String, Object> parameters,
                                         String key) {
        Object value = parameter(parameters, key);
        E.checkArgument(value instanceof String,
                        "Expect string value for parameter '%s': '%s'",
                        key, value);
        return (String) value;
    }

    public static int parameterInt(Map<String, Object> parameters,
                                   String key) {
        Object value = parameter(parameters, key);
        E.checkArgument(value instanceof Number,
                        "Expect int value for parameter '%s': '%s'",
                        key, value);
        return ((Number) value).intValue();
    }

    public static long parameterLong(Map<String, Object> parameters,
                                     String key) {
        Object value = parameter(parameters, key);
        E.checkArgument(value instanceof Number,
                        "Expect long value for parameter '%s': '%s'",
                        key, value);
        return ((Number) value).longValue();
    }

    public static double parameterDouble(Map<String, Object> parameters,
                                         String key) {
        Object value = parameter(parameters, key);
        E.checkArgument(value instanceof Number,
                        "Expect double value for parameter '%s': '%s'",
                        key, value);
        return ((Number) value).doubleValue();
    }

    public static boolean parameterBoolean(Map<String, Object> parameters,
                                           String key) {
        Object value = parameter(parameters, key);
        E.checkArgument(value instanceof Boolean,
                        "Expect boolean value for parameter '%s': '%s'",
                        key, value);
        return ((Boolean) value);
    }

    public static Directions parseDirection(Object direction) {
        if (direction.equals(Directions.BOTH.toString())) {
            return Directions.BOTH;
        } else if (direction.equals(Directions.OUT.toString())) {
            return Directions.OUT;
        } else if (direction.equals(Directions.IN.toString())) {
            return Directions.IN;
        } else {
            throw new IllegalArgumentException(String.format(
                      "The value of direction must be in [OUT, IN, BOTH], " +
                      "but got '%s'", direction));
        }
    }

    public static class AlgoTraverser extends HugeTraverser
                                      implements AutoCloseable {

        private final Job<Object> job;
        protected final ExecutorService executor;
        protected long progress;

        public AlgoTraverser(Job<Object> job) {
            super(job.graph());
            this.job = job;
            this.executor = null;
        }

        protected AlgoTraverser(Job<Object> job, String name, int workers) {
            super(job.graph());
            this.job = job;
            String prefix = name + "-" + job.task().id();
            this.executor = Consumers.newThreadPool(prefix, workers);
        }

        public void updateProgress(long progress) {
            this.job.updateProgress((int) progress);
        }

        @Override
        public void close() {
            if (this.executor != null) {
                this.executor.shutdown();
            }
        }

        protected long traverse(String sourceLabel, String sourceCLabel,
                                Consumer<Vertex> consumer) {
            Iterator<Vertex> vertices = this.vertices(sourceLabel, sourceLabel,
                                                      Query.NO_LIMIT);

            Consumers<Vertex> consumers = new Consumers<>(this.executor,
                                                          consumer);
            consumers.start();

            long total = 0L;
            while (vertices.hasNext()) {
                this.updateProgress(++this.progress);
                total++;
                Vertex v = vertices.next();
                consumers.provide(v);
            }

            consumers.await();

            return total;
        }

        protected Iterator<Vertex> vertices() {
            return this.vertices(Query.NO_LIMIT);
        }

        protected Iterator<Vertex> vertices(long limit) {
            Query query = new Query(HugeType.VERTEX);
            query.capacity(Query.NO_CAPACITY);
            query.limit(limit);
            return this.graph().vertices(query);
        }

        protected Iterator<Vertex> vertices(Object label, Object clabel,
                                            long limit) {
            return vertices(label, C_LABEL, clabel, limit);
        }

        protected Iterator<Vertex> vertices(Object label, String key,
                                            Object value, long limit) {
            Iterator<Vertex> vertices = this.vertices(label, limit);
            if (value != null) {
                vertices = filter(vertices, key, value);
            }
           return vertices;
        }

        protected Iterator<Vertex> vertices(Object label, long limit) {
            if (label == null) {
                return this.vertices(limit);
            }
            ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
            query.capacity(Query.NO_CAPACITY);
            query.limit(limit);
            if (label != null) {
                query.eq(HugeKeys.LABEL, this.getVertexLabelId(label));
            }
            return this.graph().vertices(query);
        }

        protected Iterator<Vertex> vertices(Iterator<Object> ids) {
            return new FlatMapperIterator<>(ids, id -> {
                return this.graph().vertices(id);
            });
        }

        protected Iterator<Vertex> filter(Iterator<Vertex> vertices,
                                          String key, Object value) {
            return new FilterIterator<>(vertices, vertex -> {
                boolean matched = match(vertex, key, value);
                if (!matched) {
                    this.updateProgress(++this.progress);
                }
                return matched;
            });
        }

        protected boolean match(Element elem, Object clabel) {
            return match(elem, C_LABEL, clabel);
        }

        protected boolean match(Element elem, String key, Object value) {
            // check property key exists
            this.graph().propertyKey(key);
            // return true if property value exists & equals to specified value
            Property<Object> p = elem.property(key);
            return p.isPresent() && Objects.equal(p.value(), value);
        }

        protected Iterator<Edge> edges(Directions dir) {
            HugeType type = dir == null ? HugeType.EDGE : dir.type();
            Query query = new Query(type);
            query.capacity(Query.NO_CAPACITY);
            query.limit(Query.NO_LIMIT);
            return this.graph().edges(query);
        }

        protected void drop(GraphTraversal<?, ? extends Element> traversal) {
            this.execute(traversal, () -> {
                while (traversal.hasNext()) {
                    this.updateProgress(++this.progress);
                    traversal.next().remove();
                    this.commitIfNeeded();
                }
                return null;
            });
            this.graph().tx().commit();
        }

        protected <V> V execute(GraphTraversal<?, ?> traversal,
                                Callable<V> callback) {
            long capacity = Query.defaultCapacity(MAX_QUERY_LIMIT);
            try {
                return callback.call();
            } catch (Exception e) {
                throw new HugeException("Failed to execute algorithm: %s",
                                        e, e.getMessage());
            } finally {
                Query.defaultCapacity(capacity);
                try {
                    traversal.close();
                } catch (Exception e) {
                    throw new HugeException("Can't close traversal", e);
                }
            }
        }

        protected void commitIfNeeded() {
            // commit if needed
            Transaction tx = this.graph().tx();
            Whitebox.invoke(tx.getClass(), "commitIfGtSize", tx, BATCH);
        }
    }

    public static final class TopMap<K> {

        private final long topN;
        private Map<K, MutableLong> tops;

        public TopMap(long topN) {
            this.topN = topN;
            this.tops = new HashMap<>();
        }

        public int size() {
            return this.tops.size();
        }

        public MutableLong get(K key) {
            return this.tops.get(key);
        }

        public void add(K key, long value) {
            MutableLong mlong = this.tops.get(key);
            if (mlong == null) {
                mlong = new MutableLong(value);
                this.tops.put(key, mlong);
            }
            mlong.add(value);
        }

        public void put(K key, long value) {
            this.tops.put(key, new MutableLong(value));
            // keep 2x buffer
            if (this.tops.size() > this.topN * 2) {
                this.shrinkIfNeeded(this.topN);
            }
        }

        public Set<Map.Entry<K, MutableLong>> entrySet() {
            if (this.tops.size() <= this.topN) {
                this.tops = CollectionUtil.sortByValue(this.tops, false);
            } else {
                this.shrinkIfNeeded(this.topN);
            }
            return this.tops.entrySet();
        }

        private void shrinkIfNeeded(long limit) {
            if (this.tops.size() >= limit && limit != HugeTraverser.NO_LIMIT) {
                this.tops = HugeTraverser.topN(this.tops, true, limit);
            }
        }
    }

    public static final class JsonMap {

        private final StringBuilder json;

        public JsonMap() {
            this(4 * (int) Bytes.KB);
        }

        public JsonMap(int initCapaticy) {
            this.json = new StringBuilder(initCapaticy);
        }

        public void startObject() {
            this.json.append('{');
        }

        public void endObject() {
            this.deleteLastComma();
            this.json.append('}');
        }

        public void startList() {
            this.json.append('[');
        }

        public void endList() {
            this.deleteLastComma();
            this.json.append(']');
        }

        public void deleteLastComma() {
            int last = this.json.length() - 1;
            if (last >= 0 && this.json.charAt(last) == ',') {
                this.json.deleteCharAt(last);
            }
        }

        public void appendKey(String key) {
            this.appendString(key).append(':');
        }

        public void append(long value) {
            this.json.append(value).append(',');
            this.checkSizeLimit();
        }

        public void append(String value) {
            this.appendString(value).append(',');
            this.checkSizeLimit();
        }

        public void append(Object key, long value) {
            this.append(key.toString(), value);
        }

        public void append(String key, long value) {
            this.appendString(key).append(':');
            this.json.append(value).append(',');
            this.checkSizeLimit();
        }

        public void append(Object key, Number value) {
            this.append(key.toString(), value);
        }

        public void append(String key, Number value) {
            this.appendString(key).append(':');
            this.json.append(value).append(',');
            this.checkSizeLimit();
        }

        public void append(String key, String value) {
            this.appendString(key).append(':');
            this.appendString(value).append(',');
            this.checkSizeLimit();
        }

        public void appendRaw(String key, String rawJson) {
            this.appendString(key).append(':');
            this.json.append(rawJson).append(',');
            this.checkSizeLimit();
        }

        public void appendRaw(String rawJson) {
            this.json.append(rawJson).append(',');
            this.checkSizeLimit();
        }

        public void append(Set<Entry<Id, MutableLong>> kvs) {
            for (Map.Entry<Id, MutableLong> top : kvs) {
                this.append(top.getKey(), top.getValue());
            }
        }

        private StringBuilder appendString(String str) {
            if (str.indexOf('"') >= 0) {
                str = StringEscapeUtils.escapeJson(str);
            }
            return this.json.append('"').append(str).append('"');
        }

        public void checkSizeLimit() {
            E.checkArgument(this.json.length() < MAX_RESULT_SIZE,
                            "The result size exceeds limit %s",
                            MAX_RESULT_SIZE);
        }

        public Object asJson() {
            return JsonUtil.asJson(this.json.toString());
        }
    }
}
