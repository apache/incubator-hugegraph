package com.baidu.hugegraph2.backend.query;

import java.util.Map;

/**
 * Created by jishilei on 17/3/19.
 */
public interface Query<Q extends Query<Q>>  {

   /* ---------------------------------------------------------------
    * Query Specification
    * ---------------------------------------------------------------
    */

    public Map<String,Object> conditions();

    public int limit();

    public Q has(String key, Object value);

    /**
     * Limits the size of the returned result set
     *
     * @param max The maximum number of results to return
     *
     * @return This query
     */
    public Q limit(final int max);

}
