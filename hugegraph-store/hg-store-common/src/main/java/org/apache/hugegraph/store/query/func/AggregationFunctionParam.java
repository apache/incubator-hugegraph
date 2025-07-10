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

package org.apache.hugegraph.store.query.func;

import org.apache.hugegraph.id.Id;

import lombok.Data;

@Data
public class AggregationFunctionParam {

    private AggregationFunctionType functionType;
    /**
     * the type of aggregation filed.
     * eg: sum(age): the type is integer
     */
    private FiledType filedType;
    /**
     * field id
     */
    private Id field;

    private AggregationFunctionParam(AggregationFunctionType functionType, FiledType filedType,
                                     Id filed) {
        this.functionType = functionType;
        this.filedType = filedType;
        this.field = filed;
    }

    public static AggregationFunctionParam ofCount() {
        return new AggregationFunctionParam(AggregationFunctionType.COUNT, FiledType.LONG, null);
    }

    public static AggregationFunctionParam ofSum(FiledType filedType, Id filed) {
        return new AggregationFunctionParam(AggregationFunctionType.SUM, filedType, filed);
    }

    public static AggregationFunctionParam ofMin(FiledType filedType, Id filed) {
        return new AggregationFunctionParam(AggregationFunctionType.MIN, filedType, filed);
    }

    public static AggregationFunctionParam ofMax(FiledType filedType, Id filed) {
        return new AggregationFunctionParam(AggregationFunctionType.MAX, filedType, filed);
    }

    public static AggregationFunctionParam ofAvg(FiledType filedType, Id filed) {
        return new AggregationFunctionParam(AggregationFunctionType.AVG, filedType, filed);
    }

    public enum AggregationFunctionType {
        COUNT,
        SUM,
        MIN,
        MAX,
        AVG
    }

    public enum FiledType {
        LONG("java.lang.Long"),
        INTEGER("java.lang.Integer"),
        FLOAT("java.lang.Float"),
        DOUBLE("java.lang.Double"),
        STRING("java.lang.String");

        private final String genericType;

        FiledType(String genericType) {
            this.genericType = genericType;
        }

        public String getGenericType() {
            return genericType;
        }
    }
}
