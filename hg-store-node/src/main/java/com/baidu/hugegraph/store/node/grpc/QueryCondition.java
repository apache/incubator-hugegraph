package com.baidu.hugegraph.store.node.grpc;

/**
 * @author lynn.bond@hotmail.com on 2023/2/8
 */
public interface QueryCondition {
    byte[] getStart();

    byte[] getEnd();

    byte[] getPrefix();

    int getKeyCode();

    int getScanType();

    byte[] getQuery();

    byte[] getPosition();

    int getSerialNo();
}
