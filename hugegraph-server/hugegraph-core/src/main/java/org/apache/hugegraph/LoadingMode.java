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

package org.apache.hugegraph;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.util.E;

import com.google.common.collect.ImmutableMap;

/** Same graph-instance ownership; this state is neither persisted nor distributed. */
public final class LoadingMode {

    private static final int MAX_RELEASED = 1024;

    private final String instance = UUID.randomUUID().toString();
    private final Map<String, String> active = new HashMap<>();
    private final Map<String, String> released = new HashMap<>();
    private long epoch;
    private volatile GraphMode mode = GraphMode.NONE;

    public GraphMode mode() {
        return this.mode;
    }

    public synchronized void mode(GraphMode mode) {
        Objects.requireNonNull(mode, "mode");
        E.checkState(this.active.isEmpty(), "Loading owners are active; release them before setting graph mode");
        this.mode = mode;
    }

    public synchronized Map<String, Object> capability() {
        return ImmutableMap.of("protocol", 1, "instance", this.instance, "epoch", this.epoch);
    }

    public synchronized Map<String, Object> update(String action, String instance, long epoch,
                                                   String token, String principal) {
        E.checkArgument("acquire".equals(action) || "release".equals(action),
                        "Loading owner action must be acquire or release");
        E.checkArgument(this.instance.equals(instance), "Loading owner belongs to another graph instance");
        E.checkArgument(epoch >= 0 && epoch <= this.epoch, "Invalid loading owner epoch");
        E.checkArgument(token != null && token.length() == 36 && UUID.fromString(token).toString().equals(token),
                        "Loading owner token must be a canonical UUID");
        Objects.requireNonNull(principal, "principal");
        String key = epoch + "/" + token;
        String owner = this.active.get(key);
        if (owner != null) {
            E.checkState(owner.equals(principal), "Loading owner belongs to another authenticated principal");
            if ("acquire".equals(action)) {
                return this.receipt(epoch, token, "active");
            }
            // Exhaustion must fail before changing an active registration.
            E.checkState(epoch != this.epoch || this.released.size() < MAX_RELEASED ||
                         this.epoch < Long.MAX_VALUE, "Loading owner epoch exhausted");
            this.active.remove(key);
            if (this.active.isEmpty()) {
                this.mode = GraphMode.NONE;
            }
        } else if ("acquire".equals(action)) {
            if (epoch < this.epoch || this.released.containsKey(key)) {
                return this.receipt(epoch, token, "released");
            }
            E.checkState(this.active.isEmpty() ? this.mode == GraphMode.NONE : this.mode == GraphMode.LOADING,
                         "Cannot acquire loading ownership of manually set graph mode %s", this.mode);
            this.active.put(key, principal);
            this.mode = GraphMode.LOADING;
            return this.receipt(epoch, token, "active");
        }
        // Unknown release cancels an acquire which may still be in transit.
        if (epoch == this.epoch) {
            String releasedOwner = this.released.get(key);
            E.checkState(releasedOwner == null || releasedOwner.equals(principal),
                         "Loading owner belongs to another authenticated principal");
            if (releasedOwner != null) {
                return this.receipt(epoch, token, "released");
            }
            if (this.released.size() == MAX_RELEASED) {
                E.checkState(this.epoch < Long.MAX_VALUE, "Loading owner epoch exhausted");
                this.epoch++;
                this.released.clear();
                // Old nonactive tuples are terminal; active old tuples remain registered.
            } else {
                this.released.put(key, principal);
            }
        }
        return this.receipt(epoch, token, "released");
    }

    private Map<String, Object> receipt(long epoch, String token, String state) {
        return ImmutableMap.<String, Object>builder().put("protocol", 1).put("instance", this.instance)
                           .put("epoch", epoch).put("token", token).put("state", state)
                           .put("mode", this.mode).build();
    }
}
