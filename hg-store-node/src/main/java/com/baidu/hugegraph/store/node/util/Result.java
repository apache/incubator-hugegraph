package com.baidu.hugegraph.store.node.util;

/**
 * @author lynn.bond@hotmail.com
 */
public class Result<T>{
    private Err err;
    private T t;
    public static Result of(){
       return new Result();
    }
    private Result (){}
    public T get(){
        return t;
    }
    public void set(T t){
        this.t=t;
    }
    public void err(String msg){
        this.err=Err.of(msg);
    }
}
