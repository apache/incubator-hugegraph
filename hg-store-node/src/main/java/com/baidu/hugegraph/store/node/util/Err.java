package com.baidu.hugegraph.store.node.util;

/**
 * @author lynn.bond@hotmail.com
 */
 class Err {
    private String msg;

    public static Err of(String msg){
        return new Err(msg);
    }

    private Err(String msg){
        this.msg=msg;
    }

    @Override
    public String toString() {
        return "Err{" +
                "msg='" + msg + '\'' +
                '}';
    }
}
