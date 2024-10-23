package org.apache.hugegraph.memory.allocator;

import java.util.Arrays;
import java.util.List;

public class Bar {

    public String f1;
    public List<Long> f2;

    @Override
    public String toString() {
        return f1 + "\n" + Arrays.toString(f2.toArray());
    }
}
