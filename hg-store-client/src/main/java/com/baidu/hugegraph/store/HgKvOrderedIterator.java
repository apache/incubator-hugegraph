package com.baidu.hugegraph.store;

/**
 * @author lynn.bond@hotmail.com created on 2022/03/10
 */
public interface HgKvOrderedIterator<E> extends HgKvIterator<E>, Comparable<HgKvOrderedIterator> {
    long getSequence();
}
