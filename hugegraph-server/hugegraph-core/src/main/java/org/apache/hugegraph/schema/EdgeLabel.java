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

package org.apache.hugegraph.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.schema.builder.SchemaBuilder;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Frequency;
import org.apache.hugegraph.util.E;
import com.google.common.base.Objects;

public class EdgeLabel extends SchemaLabel {

    public static final EdgeLabel NONE = new EdgeLabel(null, NONE_ID, UNDEF);

    private Id sourceLabel = NONE_ID;
    private Id targetLabel = NONE_ID;
    private Frequency frequency;
    private List<Id> sortKeys;

    public EdgeLabel(final HugeGraph graph, Id id, String name) {
        super(graph, id, name);
        this.frequency = Frequency.DEFAULT;
        this.sortKeys = new ArrayList<>();
    }

    @Override
    public HugeType type() {
        return HugeType.EDGE_LABEL;
    }

    public Frequency frequency() {
        return this.frequency;
    }

    public void frequency(Frequency frequency) {
        this.frequency = frequency;
    }

    public boolean directed() {
        // TODO: implement (do we need this method?)
        return true;
    }

    public String sourceLabelName() {
        return this.graph.vertexLabelOrNone(this.sourceLabel).name();
    }

    public Id sourceLabel() {
        return this.sourceLabel;
    }

    public void sourceLabel(Id id) {
        E.checkArgument(this.sourceLabel == NONE_ID,
                        "Not allowed to set source label multi times " +
                        "of edge label '%s'", this.name());
        this.sourceLabel = id;
    }

    public String targetLabelName() {
        return this.graph.vertexLabelOrNone(this.targetLabel).name();
    }

    public Id targetLabel() {
        return this.targetLabel;
    }

    public void targetLabel(Id id) {
        E.checkArgument(this.targetLabel == NONE_ID,
                        "Not allowed to set target label multi times " +
                        "of edge label '%s'", this.name());
        this.targetLabel = id;
    }

    public boolean linkWithLabel(Id id) {
        return this.sourceLabel.equals(id) || this.targetLabel.equals(id);
    }

    public boolean checkLinkEqual(Id sourceLabel, Id targetLabel) {
        return this.sourceLabel.equals(sourceLabel) &&
               this.targetLabel.equals(targetLabel);
    }

    public boolean existSortKeys() {
        return this.sortKeys.size() > 0;
    }

    public List<Id> sortKeys() {
        return Collections.unmodifiableList(this.sortKeys);
    }

    public void sortKey(Id id) {
        this.sortKeys.add(id);
    }

    public void sortKeys(Id... ids) {
        this.sortKeys.addAll(Arrays.asList(ids));
    }

    public boolean hasSameContent(EdgeLabel other) {
        return super.hasSameContent(other) &&
               this.frequency == other.frequency &&
               Objects.equal(this.sourceLabelName(), other.sourceLabelName()) &&
               Objects.equal(this.targetLabelName(), other.targetLabelName()) &&
               Objects.equal(this.graph.mapPkId2Name(this.sortKeys),
                             other.graph.mapPkId2Name(other.sortKeys));
    }

    public static EdgeLabel undefined(HugeGraph graph, Id id) {
        return new EdgeLabel(graph, id, UNDEF);
    }

    public interface Builder extends SchemaBuilder<EdgeLabel> {

        Id rebuildIndex();

        Builder link(String sourceLabel, String targetLabel);

        Builder sourceLabel(String label);

        Builder targetLabel(String label);

        Builder singleTime();

        Builder multiTimes();

        Builder sortKeys(String... keys);

        Builder properties(String... properties);

        Builder nullableKeys(String... keys);

        Builder frequency(Frequency frequency);

        Builder ttl(long ttl);

        Builder ttlStartTime(String ttlStartTime);

        Builder enableLabelIndex(boolean enable);

        Builder userdata(String key, Object value);

        Builder userdata(Map<String, Object> userdata);
    }
}
