/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.traversal.optimize;

import java.util.Map;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import org.apache.tinkerpop.gremlin.jsr223.SingleGremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalSourceFactory;

/**
 * ScriptTraversal encapsulates a {@link ScriptEngine} and a script which is compiled into a {@link Traversal} at {@link Admin#applyStrategies()}.
 * This is useful for serializing traversals as the compilation can happen on the remote end where the traversal will ultimately be processed.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HugeScriptTraversal<S, E> extends DefaultTraversal<S, E> {

    private static final long serialVersionUID = 4617322697747299673L;

    private final TraversalSourceFactory<TraversalSource> factory;
    private final String script;
    private final String language;
    private final Map<String, Object> bindings;
    private final Map<String, String> aliases;

    private Object result;

    public HugeScriptTraversal(TraversalSource traversalSource,
                               String language, String script,
                               Map<String, Object> bindings,
                               Map<String, String> aliases) {
        this.graph = traversalSource.getGraph();
        this.factory = new TraversalSourceFactory<>(traversalSource);
        this.language = language;
        this.script = script;
        this.bindings = bindings;
        this.aliases = aliases;
        this.result = null;
    }

    public Object result() {
        return this.result;
    }

    @Override
    public void applyStrategies() throws IllegalStateException {
        assert 0 == this.getSteps().size();
        ScriptEngine engine =
                     SingleGremlinScriptEngineManager.get(this.language);

        Bindings bindings = engine.createBindings();
        bindings.putAll(this.bindings);

        @SuppressWarnings("rawtypes")
        TraversalStrategy[] strategies = this.getStrategies().toList()
                                             .toArray(new TraversalStrategy[0]);
        bindings.put("g", this.factory.createTraversalSource(this.graph)
                                      .withStrategies(strategies));
        bindings.put("graph", this.graph);

        for(Map.Entry<String, String> entry : this.aliases.entrySet()) {
            Object value = bindings.get(entry.getValue());
            if (value == null) {
                throw new IllegalArgumentException(String.format(
                          "Invalid aliase '%s':'%s'",
                          entry.getKey(), entry.getValue()));
            }
            bindings.put(entry.getKey(), value);
        }

        try {
            Object result = engine.eval(this.script, bindings);

            if (result instanceof Admin) {
                @SuppressWarnings({ "unchecked", "resource" })
                Admin<S, E> traversal = (Admin<S, E>) result;
                traversal.getSideEffects().mergeInto(this.sideEffects);
                traversal.getSteps().forEach(this::addStep);
                this.strategies = traversal.getStrategies();
            } else {
                this.result = result;
            }
            super.applyStrategies();
        } catch (ScriptException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
