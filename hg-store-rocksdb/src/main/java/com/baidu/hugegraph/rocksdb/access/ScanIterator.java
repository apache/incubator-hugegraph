package com.baidu.hugegraph.rocksdb.access;

import java.io.Closeable;

public interface ScanIterator extends Closeable {

    public abstract class Trait {
        public static final int SCAN_ANY = 0x80;
        public static final int SCAN_PREFIX_BEGIN = 0x01;
        public static final int SCAN_PREFIX_END = 0x02;
        public static final int SCAN_GT_BEGIN = 0x04;
        public static final int SCAN_GTE_BEGIN = 0x0c;
        public static final int SCAN_LT_END = 0x10;
        public static final int SCAN_LTE_END = 0x30;
        public static final int SCAN_KEYONLY = 0x40;
        public static final int SCAN_HASHCODE = 0x100;
    }

    boolean hasNext();

    boolean isValid();

    <T> T next();

    default long count(){return 0;};

    default byte[] position(){return new byte[0];}

    default void seek(byte[] position){};

    void close();
}
