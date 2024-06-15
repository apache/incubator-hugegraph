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

package org.apache.hugegraph.store.node.grpc;

import java.util.Arrays;

import org.apache.hugegraph.store.grpc.common.ScanMethod;

/**
 * 2022/2/28
 */
class ScanQuery implements QueryCondition {

    String graph;
    String table;
    ScanMethod method;

    byte[] start;
    byte[] end;
    byte[] prefix;
    int keyCode;
    int scanType;
    byte[] query;
    byte[] position;
    int serialNo;

    private ScanQuery() {
    }

    static ScanQuery of() {
        return new ScanQuery();
    }

    @Override
    public byte[] getStart() {
        return this.start;
    }

    @Override
    public byte[] getEnd() {
        return this.end;
    }

    @Override
    public byte[] getPrefix() {
        return this.prefix;
    }

    @Override
    public int getKeyCode() {
        return this.keyCode;
    }

    @Override
    public int getScanType() {
        return this.scanType;
    }

    @Override
    public byte[] getQuery() {
        return this.query;
    }

    @Override
    public byte[] getPosition() {
        return this.position;
    }

    @Override
    public int getSerialNo() {
        return this.serialNo;
    }

    @Override
    public String toString() {
        return "ScanQuery{" +
               "graph='" + graph + '\'' +
               ", table='" + table + '\'' +
               ", method=" + method +
               ", start=" + Arrays.toString(start) +
               ", end=" + Arrays.toString(end) +
               ", prefix=" + Arrays.toString(prefix) +
               ", partition=" + keyCode +
               ", scanType=" + scanType +
               ", serialNo=" + serialNo +
               ", query=" + Arrays.toString(query) +
               ", position=" + Arrays.toString(position) +
               '}';
    }
}
