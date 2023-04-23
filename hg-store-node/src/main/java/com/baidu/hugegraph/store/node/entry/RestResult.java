package com.baidu.hugegraph.store.node.entry;

import java.io.Serializable;

import lombok.Data;

@Data
public class RestResult implements Serializable {
    public static final String OK = "OK";
    public static final String ERR = "ERR";
    String state;
    String message;
    Serializable data;
}
