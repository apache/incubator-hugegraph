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

package org.apache.hugegraph.store.query;

import java.util.HashSet;
import java.util.List;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.store.query.func.AggregationFunctionParam;

import lombok.Data;

@Data
public class StoreQueryParam {

    /**
     * 针对非 Agg：
     * 如果为空，或者size为0，则不过滤
     */
    private final PropertyList properties = PropertyList.of();
    private final boolean groupBySchemaLabel = false;
    private final SORT_ORDER sortOrder = SORT_ORDER.ASC;
    /**
     * 是否需要对key消重，针对多个query param或者index查询
     */
    private final DEDUP_OPTION dedupOption = DEDUP_OPTION.NONE;
    /**
     * 结果条数的限制
     */
    private final Integer limit = 0;
    /**
     * offset目前由server托管,理论上都应该是0
     */
    private final Integer offset = 0;
    /**
     * 抽样频率
     */
    private final double sampleFactor = 1.0;
    /**
     * 从index id中构建 base element。在No scan的case下
     */
    private final boolean loadPropertyFromIndex = false;
    /**
     * 是否解析ttl
     */
    private final boolean checkTTL = false;
    /**
     * 客户端生成，用于区分相同不同的query
     */
    private String queryId;
    /**
     * the graph
     */
    private String graph;
    /**
     * the table name
     */
    private String table;
    /**
     * 聚合函数列表
     */
    private List<AggregationFunctionParam> funcList;
    /**
     * 分组列表, 同时也是properties
     */
    private List<Id> groupBy;
    /**
     * 排序字段。
     * 优先级低于 property.
     * Agg: 如果不在group by中，id是无效的
     * 非Agg: 如果不在property中，id是无效的
     */
    private List<Id> orderBy;
    /**
     * 过滤条件
     */
    private ConditionQuery conditionQuery;
    /**
     * 暂不实现
     */
    private List<Integer> having;
    private StoreQueryType queryType;
    private List<QueryTypeParam> queryParam;
    /**
     * 用于非 order by, 非Agg的查询中
     */
    private byte[] position;
    /**
     * 将olap表中对应的属性，添加到HgElement上 (Vertex)
     */
    private List<Id> olapProperties;
    /**
     * 索引, 每个内层的元素为and关系，外层为or关系。IndexRange是一个range 查询
     * 如果 scanType是 INDEX_SCAN，则需要回查原表。
     */
    private List<List<QueryTypeParam>> indexes;

    private static void isFalse(boolean expression, String message) {

        if (message == null) {
            throw new IllegalArgumentException("message is null");
        }

        if (expression) {
            throw new IllegalArgumentException(message);
        }
    }

    private static <E> boolean isEmpty(List<E> list) {
        return list == null || list.size() == 0;
    }

    public void checkQuery() {
        isFalse(queryId == null, "query id is null");
        isFalse(graph == null, "graph is null");
        isFalse(table == null, "table is null");

        isFalse(queryType == null, "queryType is null");

        isFalse(queryType == StoreQueryType.PRIMARY_SCAN && isEmpty(queryParam),
                "query param is null when PRIMARY_SCAN");
        // no scan & index scan should have indexes
        isFalse(queryType == StoreQueryType.NO_SCAN && isEmpty(indexes),
                "ScanType.NO_SCAN without indexes");
        isFalse(queryType == StoreQueryType.NO_SCAN &&
                (indexes.size() != 1 || indexes.get(0).size() != 1),
                "ScanType.NO_SCAN only support one index");
        isFalse(loadPropertyFromIndex &&
                (isEmpty(indexes) || indexes.size() != 1 || indexes.get(0).size() != 1),
                " loadPropertyFromIndex only support one(must be one) index in no scan");

        isFalse(queryType == StoreQueryType.INDEX_SCAN && isEmpty(indexes),
                "ScanType.INDEX_SCAN without indexes ");

        isFalse(!isEmpty(groupBy) && !isEmpty(properties.getPropertyIds()) &&
                !new HashSet<>(groupBy).containsAll(properties.getPropertyIds()),
                "properties should be subset of groupBy");

        isFalse(!isEmpty(groupBy) && !isEmpty(orderBy) &&
                !new HashSet<>(groupBy).containsAll(orderBy),
                "order by should be subset of groupBy");

        // isFalse(properties.isEmptyId() && ! queryParam.stream().allMatch(p -> p.isIdScan()),
        //       "empty property only apply id scan");

        // todo: just group by, no aggregations ??
        if (funcList != null) {
            for (var func : funcList) {
                if (func.getFunctionType() == AggregationFunctionParam.AggregationFunctionType.SUM
                    ||
                    func.getFunctionType() == AggregationFunctionParam.AggregationFunctionType.MAX
                    ||
                    func.getFunctionType() == AggregationFunctionParam.AggregationFunctionType.MIN
                    || func.getFunctionType() ==
                       AggregationFunctionParam.AggregationFunctionType.AVG) {
                    isFalse(func.getField() == null,
                            func.getFunctionType().name() + " has no filed value");
                }

                if (func.getFunctionType() ==
                    AggregationFunctionParam.AggregationFunctionType.SUM) {
                    //  ||func.getFunctionType() == AggregationFunctionParam
                    //  .AggregationFunctionType.AVG){
                    isFalse(func.getFiledType() == AggregationFunctionParam.FiledType.STRING,
                            func.getFunctionType().name() + " can not apply a String type");
                }
            }
        }

        isFalse(limit < 0, "limit should be greater than 0");
        isFalse(sampleFactor < 0 || sampleFactor > 1, "sample factor out of range [0-1]");
    }

    public enum DEDUP_OPTION {
        NONE,
        /**
         * 模糊去重，使用bitmap
         */
        DEDUP,
        /**
         * 前N行保证精确去重,之后的非精确
         */
        LIMIT_DEDUP,
        /**
         * 精确去重，保证准确性
         */
        PRECISE_DEDUP
    }

    public enum SORT_ORDER {
        ASC,
        DESC,
        /**
         * 仅仅针对全部是ID查询，保持原始传入的id顺序
         */
        STRICT_ORDER
    }

}
