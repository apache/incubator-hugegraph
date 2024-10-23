package org.apache.hugegraph.memory.allocator;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Foo {

    public int f1;
    public List<Integer> f2;
    public Map<String, Integer> f3;
    public List<Bar> f4;

    @Override
    public String toString() {
        return f1 + "\n" + Arrays.toString(f2.toArray()) + "\n" + Arrays.toString(f4.toArray());
    }
}
