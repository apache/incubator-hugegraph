package org.apache.hugegraph.pd.pulse;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.watch.WatchListener;

public class DefaultPulseListener<T> implements PulseListener<PulseResponse> {
    private final List<WatchListener<T>> watchListeners = new CopyOnWriteArrayList<>();

    private final Function<PulseResponse, T> responseParser;

    public DefaultPulseListener(Function<PulseResponse, T> responseParser) {
        this.responseParser = responseParser;
    }

    public void addWatchListener(WatchListener<T> listener) {
        this.watchListeners.add(listener);
    }

    public void removeWatchListener(WatchListener<T> listener) {
        this.watchListeners.remove(listener);
    }

    private void raiseData(PulseResponse response) {
        for (WatchListener<T> listener : this.watchListeners) {
            listener.onNext(this.responseParser.apply(response));
        }
    }

    private void raiseError(Throwable throwable) {
        for (WatchListener<T> listener : this.watchListeners) {
            listener.onError(throwable);
        }
    }

    private void raiseCompleted() {
        for (WatchListener<T> listener : this.watchListeners) {
            listener.onCompleted();
        }
    }

    public void onNext(PulseResponse response) {
    }

    public void onNotice(PulseServerNotice<PulseResponse> notice) {
        raiseData(notice.getContent());
    }

    public void onError(Throwable throwable) {
        raiseError(throwable);
    }

    public void onCompleted() {
        raiseCompleted();
    }
}
