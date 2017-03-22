package com.baidu.hugegraph2.backend.query;

/**
 * Created by jishilei on 17/3/19.
 */
public interface Query {

    public static final int NO_LIMIT = Integer.MAX_VALUE;

    /**
     * Whether this query has a defined limit
     *
     * @return
     */
    public boolean hasLimit();

    /**
     *
     * @return The maximum number of results this query should return
     */
    public int limit();

}
