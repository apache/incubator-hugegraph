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

package org.apache.hugegraph.store.node.grpc.query.stages;

import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.store.node.grpc.query.QueryStage;
import org.apache.hugegraph.store.node.grpc.query.QueryUtil;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResult;

import lombok.extern.slf4j.Slf4j;

/**
 * 反序列化
 */
@Slf4j
public class DeserializationStage implements QueryStage {

    private HugeGraphSupplier graph;
    private String table;

    @Override
    public void init(Object... objects) {
        this.table = (String) objects[0];
        this.graph = (HugeGraphSupplier) objects[1];
    }

    /**
     * 将 PipelineResult 处理为 PipelineResult，将查询结果转换为图元素。
     *
     * @param result 查询结果
     * @return 转换后的 PipelineResult，如果查询结果为空则返回 null。
     */
    @Override
    public PipelineResult handle(PipelineResult result) {
        if (result.isEmpty()) {
            return result;
        }
        var column = result.getColumn();
        if (column.value == null) {
            return null;
        }
        try {
            var element = QueryUtil.parseEntry(this.graph,
                                               BackendColumn.of(column.name, column.value),
                                               QueryUtil.isVertex(this.table));
            return new PipelineResult(element);
        } catch (Exception e) {
            log.error("Deserialization error: {}", graph, e);
            return null;
        }
    }

    @Override
    public String getName() {
        return "DESERIALIZATION_STAGE";
    }

}
