package com.baidu.hugegraph.store.node.grpc;

import com.baidu.hugegraph.store.grpc.common.ScanMethod;

import java.util.Arrays;

/**
 * @author lynn.bond@hotmail.com on 2022/2/28
 */
class ScanQuery implements QueryCondition{
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

    static ScanQuery of() {
        return new ScanQuery();
    }

    private ScanQuery() {
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
