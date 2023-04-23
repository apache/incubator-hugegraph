package com.baidu.hugegraph.store.business;

import com.baidu.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import com.baidu.hugegraph.rocksdb.access.ScanIterator;
import com.baidu.hugegraph.store.term.Bits;

import java.util.Arrays;

public class InnerKeyFilter<T extends BackendColumn> implements ScanIterator {
    ScanIterator iterator;
    final int codeFrom;
    final int codeTo;
    T current = null;

    //是否进行code过滤，启动该选项，返回key的尾部包含code
    final boolean codeFilter;
    public InnerKeyFilter(ScanIterator iterator){
        this.iterator = iterator;
        this.codeFrom = Integer.MIN_VALUE;
        this.codeTo = Integer.MAX_VALUE;
        this.codeFilter = false;
        moveNext();
    }

    public InnerKeyFilter(ScanIterator iterator, int codeFrom, int codeTo){
        this.iterator = iterator;
        this.codeFrom = codeFrom;
        this.codeTo = codeTo;
        this.codeFilter = true;
        moveNext();
    }

    private void moveNext(){
        current = null;
        if (codeFilter) {
            while (iterator.hasNext()) {
                T t = iterator.next();
                int code = Bits.getShort(t.name, t.name.length - Short.BYTES);
                if (code >= codeFrom && code < codeTo) {
                    current = t;
                    break;
                }
            }
        } else {
            if (iterator.hasNext())
                current = iterator.next();
        }
    }
    @Override
    public boolean hasNext() {
        return current!= null;
    }

    @Override
    public boolean isValid() {
        return iterator.isValid();
    }

    @Override
    public T next() {
        T column = current;
        if (!codeFilter)
            // 去掉图ID和hash后缀
            column.name = Arrays.copyOfRange(column.name, Short.BYTES,
                    column.name.length - Short.BYTES);
        else// 去掉图ID
            column.name = Arrays.copyOfRange(column.name, Short.BYTES,
                    column.name.length);
        moveNext();
        return column;
    }

    @Override
    public void close() {
        iterator.close();
    }

    @Override
    public long count() {
        return iterator.count();
    }
}
