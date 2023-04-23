package com.baidu.hugegraph.store;

/**
 * Return the amount of records returned by one query in pageable-query.
 *
 * @author lynn.bond@hotmail.com created on 2021/10/24
 */
public interface HgPageSize {
    long getPageSize();
    default boolean isPageEmpty(){
        return false;
    }
}
