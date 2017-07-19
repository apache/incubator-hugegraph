package com.baidu.hugegraph.type;

public interface TriFunction <T1, T2, T3, R> {
    public R apply(T1 v1, T2 v2, T3 v3);
}
