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

package org.apache.hugegraph;

import java.util.Collection;
import java.util.List;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.util.DateUtil;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.VertexLabel;

/**
 * Acturally, it would be better if this interface be called
 * "HugeGraphSchemaSupplier".
 */
public interface HugeGraphSupplier {

    public List<String> mapPkId2Name(Collection<Id> ids);

    public List<String> mapIlId2Name(Collection<Id> ids);

    public PropertyKey propertyKey(Id key);

    public Collection<PropertyKey> propertyKeys();

    public VertexLabel vertexLabelOrNone(Id id);

    public boolean existsLinkLabel(Id vertexLabel);

    public VertexLabel vertexLabel(Id label);

    public VertexLabel vertexLabel(String label);


    public default EdgeLabel edgeLabelOrNone(Id id) {
        EdgeLabel el = this.edgeLabel(id);
        if (el == null) {
            el = EdgeLabel.undefined(this, id);
        }
        return el;
    }
    public EdgeLabel edgeLabel(Id label);

    public EdgeLabel edgeLabel(String label);

    public IndexLabel indexLabel(Id id);

    public Collection<IndexLabel> indexLabels();

    public String name();

    public HugeConfig configuration();

    default long now() {
        return DateUtil.now().getTime();
    }
}
