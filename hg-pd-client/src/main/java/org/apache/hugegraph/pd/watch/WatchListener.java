package org.apache.hugegraph.pd.watch;

public interface WatchListener<T> {
    void onNext(T paramT);

    void onError(Throwable paramThrowable);

    default void onCompleted() {}
}
