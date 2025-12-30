package org.apache.hugegraph.vector;

public interface SequenceGenerator {
    long next();
    void init(long lastSeq);
    long getCurrent();
}
