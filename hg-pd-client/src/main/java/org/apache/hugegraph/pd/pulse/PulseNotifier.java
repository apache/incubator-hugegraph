package org.apache.hugegraph.pd.pulse;


import java.io.Closeable;

public interface PulseNotifier<T> extends Closeable {

    void notifyServer(T paramT);

    void crash(String paramString);
}
